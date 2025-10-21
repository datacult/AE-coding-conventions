<!--
If you need guidance on how to use the template, please see [PR Guidelines](https://docs.google.com/document/d/1gp8Y_mX-fmYLWH-Ia5JRcLIRshuovSqdFMDgZrF2gi0/edit)
-->

### Description & Motivation
<!--
Please describe the goal of your PR. This is the intro to your PR and should allow the reviewer to quickly be able to understand the reason for opening this PR. If your actual code is the “how”, the description is the “what” and “why.” Include links to any tasks or documentation that may be relevant and helpful for review
-->

### New Models
<!--
Outline the purpose, grain, core fields, and additional details surrounding the logic used in the model where relevant.
-->

### Changes to Existing Models
<!--
Outline changes for each model that was updated with added detail about changes that were made.
-->

### Dependencies
<!--
Include a screenshot of the updated DAG through at least 2 steps downstream of the update model(s). This is intended to highlight what models are impacted by the changes - where relevant for changes made early in the DAG, not primary end models impacted here as well.
-->

### Validation of models
<!--
 How does the model output compare to the existing source of truth / expected behavior? This should include links to any QA spreadsheets, screenshots of changes in outputs, example queries used for validation, and any other relevant information surrounding the validation of model updates. Please refer to this QA workflow [document here](https://docs.google.com/document/d/1hkok6qewy0Ba6SaQFKhLHOVAz4qYeF-fVBalmPuE8tY/edit?tab=t.0#heading=h.l92c7y7d7r6a).
-->

### Checklist
<!--
ALL ITEMS SHOULD BE COMPLETE BEFORE REQUESTING REVIEW
-->

- [ ] My pull request represents one logical piece of work
- [ ] I ran and tested my models using dbt run & dbt test (include screenshot below)
- [ ] I have documented QA of affected models in this PR
- [ ] I have added appropriate tests and documentation to any new models/fields.
- [ ] I have materialized my models appropriately.
- [ ]  My SQL follows the [Data Culture style guide](https://github.com/datacult/AE-coding-conventions/blob/main/dc-sql_style.md)
