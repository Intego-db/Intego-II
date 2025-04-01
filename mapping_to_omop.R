

# REMARKS ----------------------------------------------------------------
# Still some omop variables missing in certain table (concept_id, value_as_number, ...)


library(arrow)
library(dplyr)
library(data.table)


clean_environment <- function(){
  
  rm(list=setdiff(ls(envir = .GlobalEnv),c("read_file","clean_environment","intego_code_glossary")),envir = .GlobalEnv)
  
}

read_file <- function(file_name){
  
  dir_path <- "data_release_0-1"
  
  require(arrow)
  
  data <- read_parquet(file.path(dir_path,paste0(file_name,".parquet")))
  if(!is.data.table(data)) data <- as.data.table(data)
  return(data)
}

# Intego code glossary 
intego_code_glossary <- as.data.table(read.csv("code_glossary/intego_code_glossary.csv"))


# 1) condition_occurrence OMOP Table -----
file1 <- "diagnoses" 
file2 <- "history_chronic_diseases"

diag <- read_file(file1)
chronic_dis <- read_file(file2)

colnames(diag)
colnames(chronic_dis)

condition_occurrence <- rbindlist(list(diag,chronic_dis))
condition_occurrence <- unique(condition_occurrence[,.(
  person_id = patient_id,
  provider_id = practice_id,
  condition_start_date = date,
  condition_source_value = icd10)])
condition_occurrence <- condition_occurrence[!is.na(condition_source_value)]

# ICD-10 should be mapped to SNOMED-CT codes 
condition_occurrence <- merge(
  condition_occurrence,
  intego_code_glossary[source_vocabulary=="ICD10"][,.(source_code,standard_code)],
  all.x=T,
  by.x = "condition_source_value", by.y = "source_code"
)

condition_occurrence <- condition_occurrence[!is.na(standard_code)]
condition_occurrence <- condition_occurrence[,.(person_id, provider_id, condition_start_date,condition_concept_id = standard_code)]

clean_environment()


# 2) death OMOP Table -----
file <- "patient_master"
death <- read_file(file)
death <- unique(death[,.(patient_id,death_date)])
death <- death[!is.na(death_date)]
head(death)


# 3) drug_exposure OMOP Table ----
file1 <- "history_vaccines"
file2 <- "prescriptions"

vaccines <- read_file(file1)
prescriptions <- read_file(file2)
colnames(vaccines);colnames(prescriptions)

drug_exposure <- rbindlist(list(
  unique(vaccines[,.(patient_id,practice_id,start_date = date, vac_code)]),
  unique(prescriptions[,.(patient_id,practice_id,start_date,end_date, vac_code = pres_cnk, pres_type,dosage_text)])
), fill = TRUE)

drug_exposure <- drug_exposure[!is.na(vac_code)]

colnames(drug_exposure)
drug_exposure <- drug_exposure[,.(
  person_id = patient_id,
  provider_id = practice_id,
  drug_exposure_start_date = start_date,
  drug_exposure_end_date = end_date,
  drug_source_value = vac_code,
  refills = pres_type,
  sig = dosage_text
)]

# Replace drug_source_value (ATC / CNK) with drug_concept_id (RxNorm) 
# - CNK and ATC should be mapped to RXNorm. 
# - ATC-RxNorm mapping, CNK needs to be mapped to ATC first 
drug_exposure <- drug_exposure[,source_vocabulary := fcase(
  grepl("[0-9]{7}",drug_source_value), "CNK",
  default = "ATC"
)]

drug_exposure_atc <- drug_exposure[source_vocabulary == "ATC"]
drug_exposure_cnk <- drug_exposure[source_vocabulary == "CNK"]

# ATC to RxNorm
drug_exposure_atc <- merge(
  drug_exposure_atc,
  intego_code_glossary[source_vocabulary == "ATC"][,.(source_code,standard_code)][!is.na(standard_code)],
  all.x=T,
  by.x = "drug_source_value", by.y = "source_code"
)

# CNK to ATC
drug_exposure_cnk <- merge(
  drug_exposure_cnk,
  intego_code_glossary[source_vocabulary == "CNK"][,.(source_code,standard_code)][!is.na(standard_code)],
  # all.x=T,
  by.x = "drug_source_value", by.y = "source_code"
)

drug_exposure_cnk <- drug_exposure_cnk[!is.na(standard_code)]
drug_exposure_cnk$drug_source_value <- drug_exposure_cnk$standard_code
drug_exposure_cnk <- drug_exposure_cnk[,standard_code := NULL]

# ATC to RxNorm
drug_exposure_cnk <- merge(
  drug_exposure_cnk,
  intego_code_glossary[source_vocabulary == "ATC"][,.(source_code,standard_code)][!is.na(standard_code)],
  all.x=T,
  by.x = "drug_source_value", by.y = "source_code"
)

drug_exposure <- rbindlist(list(drug_exposure_atc, drug_exposure_cnk))

drug_exposure <- drug_exposure[!is.na(standard_code)]
drug_exposure <- unique(drug_exposure[,.(person_id, provider_id, drug_exposure_start_date,
                                         drug_exposure_end_date, refills, sig, drug_concept_id = standard_code)])

head(drug_exposure)

clean_environment()


# 4) measurements OMOP Table -----

file1 <- "patient_body_measures"
file2 <- "laboratory_results"
file3 <- "measurements"

body_measures <- read_file(file1)
lab <- read_file(file2)
measurements <- read_file(file3)

colnames(body_measures)
head(body_measures)
body_measures <- data.table::melt(body_measures, measure.vars = c("length_cm","weight_kg","bmi","abd_circum"))
body_measures <- body_measures[!is.na(value)]

library(stringr)
var_unit_df <- str_split_fixed(body_measures$variable,"_",2)
body_measures$variable <- var_unit_df[,1]
body_measures$unit <- var_unit_df[,2]

body_measures <- unique(body_measures[,.(
  person_id = patient_id, 
  provider_id = practice_id,
  measurement_date = date,
  measurement_concept_id = variable,
  # value_as_number = value,
  value_source_value = value,
  unit_source_value = unit)])

colnames(lab)
df_ref_range <- unique(lab[,.(ref_range)])
df_ref_range <- df_ref_range[!is.na(ref_range)]
ref_range_split <- str_split_fixed(df_ref_range$ref_range,"-",2)
df_ref_range$low <- ref_range_split[,1]
df_ref_range$high <- ref_range_split[,2]
idx_high <- which(grepl("<",df_ref_range$low))
df_ref_range$high[idx_high] <- df_ref_range$ref_range[idx_high]
df_ref_range$low[idx_high] <- NA
df_ref_range <- as.data.table(df_ref_range)
lab <- merge(lab, df_ref_range, by = "ref_range")
colnames(lab)
lab <- unique(lab[,.(
  person_id = patient_id,
  provider_id = practice_id,
  measurement_date = date,
  measurement_source_value = medidoc_code,
  value_source_value = lab_result,
  unit_source_value = lab_unit,
  range_low = low,
  range_high = high
)])

# Map lab medidoc to LOINC (measurement_concept_id)
lab <- merge(
  lab,
  intego_code_glossary[source_vocabulary == "MEDIDOC"][,.(source_code,standard_code)][!is.na(standard_code)],
  all.x=T,
  by.x = "measurement_source_value", by.y = "source_code"
)

lab <- lab[!is.na(standard_code)]
lab <- unique(lab[
  ,
  .(person_id,
    provider_id,
    measurement_date,
    value_source_value,
    unit_source_value,
    range_low,
    range_high,
    measurement_concept_id = standard_code)])

colnames(measurements)
head(measurements)
measurements <- unique(measurements[,.(
  person_id = patient_id,
  provider_id = practice_id,
  measurement_date = date,
  measurement_concept_id = measure_code,
  # value_as_number = measure_value,
  value_source_value = measure_value,
  unit_source_value = measure_unit
)])

measurement_omop <- rbindlist(list(
  body_measures,
  lab,
  measurements
), fill = TRUE)

colnames(measurement_omop)
head(measurement_omop)

clean_environment()


# 5) observation OMOP Table ----

file1 <- "patient_lifestyle"
file2 <- "history_family"
file3 <- "history_allergies"
file4 <- "history_intolerance"
file5 <- "absence_certificates"

patient_lifestyle <- read_file(file1)
history_family <- read_file(file2)
allergies <- read_file(file3)
intolerance <- read_file(file4)
absence <- read_file(file5)

colnames(patient_lifestyle)
patient_lifestyle <- melt(patient_lifestyle, measure.vars = c("physical_activity","smoking_status","alcohol_use"))
patient_lifestyle <- patient_lifestyle[!is.na(value)]
patient_lifestyle <- patient_lifestyle[,observation_concept_id := fcase(
  grepl("physical_activity",variable), 4195380,
  grepl("smoking_status",variable),43054909 ,
  grepl("alcohol_use",variable),40771103 
)]

patient_lifestyle <- patient_lifestyle[,.(
  person_id = patient_id,
  provider_id = practice_id,
  observation_date = date,
  observation_concept_id,
  value_as_concept_id = value
)]


colnames(history_family) 
history_family <- history_family[,fam_icpc := NULL]
history_family <- history_family[,.(
  person_id = patient_id,
  provider_id = practice_id,
  value_as_concept_id = fam_relation,
  observation_source_value= fam_icd10
)]
history_family <- unique(history_family)

# To add by mapping? 
history_family$observation_concept_id <- NA

colnames(allergies)
allergies <- unique(allergies[,.(
  person_id = patient_id,
  provider_id = practice_id,
  observation_date = date,
  observation_source_value = allergen_text
)])

colnames(intolerance)
intolerance <- unique(intolerance[,.(
  person_id = patient_id,
  provider_id = practice_id,
  observation_date = date,
  observation_source_value = medintol_text
)])

colnames(absence)
absence <- unique(absence[,.(
  person_id = patient_id,
  provider_id = practice_id,
  observation_date = start_date,
  value_as_number = end_date-start_date
)])
absence$observation_concept_id <- 4141545

observation <- rbindlist(list(
  patient_lifestyle,
  history_family,
  allergies,
  intolerance,
  absence
), fill = TRUE)



# 6) person OMOP Table ----
file1 <- "patient_master"
file2 <- "patient_dynamic"

patient_master <- read_file(file1)
patient_dynamic <- read_file(file2)

colnames(patient_master)
patient_master <- unique(patient_master[,.(
  person_id = patient_id,
  gender_concept_id = gender,
  year_of_birth  = birth_year
)])

patient_dynamic <- unique(patient_dynamic[,.(
  person_id = patient_id,
  provider_id = practice_id,
  location_id = postal_code
)])

person <- merge(patient_master, patient_dynamic)


# 7) procedure_occurrence OMOP Table ---- 

file1 <- "history_procedures"
file2 <- "imaging_orders"
file3 <- "physiotherapy_prescriptions"

history_procedures <- read_file(file1)
imaging_orders <- read_file(file2)
physiotherapy <- read_file(file3)

colnames(history_procedures)
history_procedures <- history_procedures[,.(
  person_id = patient_id,
  provider_id = practice_id,
  procedure_date = date,
  procedure_source_value = proced_text
)]

colnames(imaging_orders)
imaging_orders <- imaging_orders[,.(
  person_id = patient_id,
  provider_id = practice_id,
  procedure_date = date,
  procedure_concept_id = 4180938
)]

colnames(physiotherapy)
physiotherapy <- physiotherapy[,.(
  person_id = patient_id,
  provider_id = practice_id,
  procedure_date = date,
  procedure_concept_id = 4238738
)]


procedure_occurence <- rbindlist(list(
  history_procedures,
  imaging_orders,
  physiotherapy
), fill = TRUE)

