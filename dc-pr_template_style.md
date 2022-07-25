If you need guidance on how to use the template, please see [PR Guidelines](https://docs.google.com/document/d/1gp8Y_mX-fmYLWH-Ia5JRcLIRshuovSqdFMDgZrF2gi0/edit)

# Data Culture Pre-Pull Request Submission Checklist 

- [ ]  Branch Naming Convention: Make it clear what the branch represents
- [ ]  Clear commenting in the code
- [ ]  Dev Testing and Validation
    - [ ]  Does the code run   
    - [ ]  How long does the code take to run
    - [ ]  Tests for data quality and do they pass
    - [ ]  Compared results with available offline reports / numbers make sense to the business



# Data Culture Pull Request Submission Checklist

- [ ]  Git Commit Message: In case you have a few changes in the same commit, be descriptive and concise in your bullet points for every change you have done
- [ ]  Provide a short summary in the Title
- [ ]  Description and Motivation
- [ ]  Screenshots
    - [ ]  Include a screenshot of the relevant section of the updated DAG
- [ ]  Validation of Models
    - [ ]  Include screenshots of dbt running, tests passing, run times
- [ ]  Changes to Existing Models
- [ ]  Any additional callouts you may have for specific code you need reviewed (ex: incurring tech debt, special logic for edge-case handling, special business logic, etc.)
- [ ]  CI Tests have completed successfully



##### Code Checklist (Put an `x` in all the items that apply, make notes next to any that haven't been addressed, and remove items that are not relevant to this PR)

- [ ]  My request represents one logical piece of work and is at most ~250 lines of code
- [ ]  My commits are related to the pull request
- [ ]  My SQL follows the [Data Culture style guide](https://github.com/datacult/AE-coding-conventions/blob/main/dc-sql_style.md)
- [ ]  I have materialized my models appropriately
- [ ]  I have added appropriate tests and documentation to new models

##### The following checklist is for RedShift Warehouses
- [ ] I have updated the README file
- [ ] I have added sort and dist keys to models materialized as tables
- [ ] I have validated the SQL in any late-binding views


# Data Culture Pull Request Review Checklist

- [ ]  Provide a description of how you believe / are reading the code to function
- [ ]  Feedback to the code provides a suggested solution
    - [ ]  Leave actionable / explicit / complete comments 
    - [ ]  Share references when requesting changes where appropriate
    - [ ]  Combine similar comments
    - [ ]  Replace “you” with “we”
- [ ]  Follow Commenting Format:   [color code][label] [decorations]: [subject] 
                                [discussion]



#### Considerations For Reviewing 
- [ ]  **Design**: Is the code well-designed and appropriate for your system?
- [ ]  **Functionality**: Does the code behave as the author intended?
    - [ ]  Data returns expected results 
    - [ ]  Materializations are designated appropriately
- [ ]  **Complexity**: Could the code be made simpler? Would another developer be able to easily understand and use this code when they come across it in the future?
    - [ ]  Is there repeat code
    - [ ]  Can the DAG be modified so fields can be defined earlier and cascaded more seamlessly
    - [ ]  Should parts of the code use macros or dbt-utils
    - [ ]  Can CTEs be models of their own
    - [ ]  Could this code be easily scaled
- [ ]  **Tests**: Does the code have correct and well-designed functional tests?
    - [ ]  Uniqueness and not null at a minimum
- [ ]  **Naming**: Did the developer choose clear names for variables, models, etc.?
    - [ ]  Do model names appropriately represent the grain of the model?
    - [ ]  Do the new or changed model names follow the conventions of the project?
    - [ ]  Are fields in models straightforward / descriptive and to the point 
- [ ]  **Comments**: Are the comments clear and useful?
- [ ]  **Style**: Does the code follow [Data Culture style guide](https://github.com/datacult/AE-coding-conventions/blob/main/dc-sql_style.md)?
- [ ]  **Documentation**: Did the developer also update relevant documentation?