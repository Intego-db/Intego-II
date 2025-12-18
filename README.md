# Intego-II
This repository serves as a companion to the Intego-II primary care database, providing essential resources to facilitate research and data standardization. A detailed description of the database is published in the International Journal of Epidemiology: https://doi.org/10.1093/ije/dyaf200.

## Repository Contents
* Code Glossary: A mapping table between Intego-II source codes and standard codes as defined in the OMOP CDM.
* OMOP Mapping Script: An R script to align Intego-II data with the OMOP CDM.

Additional resources may be included in the future as the database and its applications evolve.
For more information on Intego-II, visit www.intego.be.

## Code glossary
This code glossary provides an overview of how clinical codes used in the Intego-II database are standardized and mapped to commonly used international clinical terminologies. Below is a detailed explanation of each column in the glossary:

| Column Name           | Description                                                                                                                                                      |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| source_code          | The original clinical code as recorded in the Intego-II database. These are the native codes used in the data collection software.                               |
| source_vocabulary    | The classification system or vocabulary from which the source_code originates (e.g., ICPC-2, ICD-10, Medidoc).                                                  |
| frequency            | The number of times this specific source_code appears in the Intego-II database, i.e., the number of associated records.                                        |
| source_concept_class | The type or category to differentiate Concepts within a given Vocabulary.                                                           |
| source_name          | A human-readable description or label for the source_code. This is usually a term used by general practitioners during data entry.                               |
| validity             | An indication of whether the source code is still valid or has been deprecated. This helps users assess whether the code is up-to-date.                          |
| standard_code        | The corresponding standardized code that the source_code has been mapped to, using internationally recognized terminologies (e.g., SNOMED CT, ICD-10).          |
| standard_name        | A descriptive label for the standard_code, reflecting the harmonized term from the target vocabulary.                                                           |
| standard_vocabulary  | The name of the standardized vocabulary to which the source_code has been mapped (e.g., SNOMED CT, LOINC).                                                     |
| mapping_resource     | The source used for the mapping process.                                                                                                                         |

An example from the mapping table: 

| source_code | source_vocabulary | frequency | source_concept_class | source_name                                                    | validity | standard_code        | standard_name                                                | standard_vocabulary | mapping_resource  |
|-------------|-------------------|-----------|-----------------------|----------------------------------------------------------------|----------|----------------------|--------------------------------------------------------------|----------------------|-------------------|
| J11.1       | ICD10            | 692609    | ICD10 code            | Influenza with other respiratory manifestations, virus not identified | valid    | 10685111000119102    | Upper respiratory tract infection caused by Influenza virus | SNOMED              | cdm_v5_20240830  |

# Technical Details of the Secure Research Environment (SRE)

Intego-II data is accessed through a **Secure Research Environment (SRE)** managed by [Healthdata.be](https://www.healthdata.be), ensuring compliance with Belgian and international privacy and security standards. Below are the key technical aspects for researchers:

---

## 1. Access and Platform
- Access is provided via **Citrix Workspace** with **mandatory two-factor authentication (2FA)**.
- The environment currently operates on an **on-premise Windows-based infrastructure**.
- **Upcoming update:** Migration to **Microsoft Azure Virtual Desktop** for improved scalability, security, and compliance.

---

## 2. Security and Governance
- Managed by **Healthdata.be**, under the governance of **eHealth** and **Sciensano**.
- Research activities occur in a **controlled, internet-disabled environment**.
- All sessions and actions are logged for **audit and compliance**.
- Data governance and security measures are approved by the **Belgian Information Security Committee (Social Security and Health Chamber)**.
- **CareConnect**, the EMR software used in data collection, is validated by **Corilus**.

---

## 3. Available Software
- **Data Analysis:**  
  - **R** (with commonly used packages)  
  - **Python** (via Anaconda distribution)  
- **Documentation:**  
  - **LibreOffice**  
- Additional software can be installed upon request and approval.

---

## 4. Data Storage and Structure
- **Source data**: Delivered as `.csv` text files.  
- **Processed data**: Stored as `.parquet` files for efficient and reproducible research workflows.  
- Each quarterly release is stored in a **versioned directory structure** to ensure full traceability.

---

## 5. Data Egress (Export) Policy
- Only **aggregated results** may be exported—**no individual-level data** is allowed.
- Export requests are submitted through the **Intego data manager** for review.
- Datasets are validated against **Healthdata.be’s export principles** before approval.
- Approved exports include a **metadata file** for documentation and audit purposes.
- Final delivery to the researcher occurs after approval by **Healthdata.be**.
- All exports are logged in a **secure audit trail**.

---

### More Information
For updates, user guides, and detailed documentation, please refer to this repository regularly.
