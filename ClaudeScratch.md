ClaudeScratchThe team discussed using the meeting as a pairing session for Avatica Phase 3 as it was identified as a good, achievable task for Claude. They then reviewed the status of the Avatica removal project based on the four-phase plan detailed in the "JDBC Python planning" Google Doc.1

Status Relative to Planning Document Phases:
Phase 1: Completed, which included replacing Avatica's core classes with custom implementations and replacing boilerplate code.1
Phase 2: Partially complete. Metadata handling is finished. The custom type conversion (for SQL type and type converter) supports all metadata limited types, but needs more extensions to fully support result sets.1
Phase 3: Not yet implemented. This is the main portion that is used for functions like executing queries.1
Phase 4: Considered partially complete as a separate phase is not necessary. The tasks involved, such as updating the build configuration to remove the dependency, removing Avatica imports, and updating tests, will be done as part of the other phases.1
Next Steps for Phase 3:

The current plan is to spend a few hours starting in "planning mode" with Claude to gather context, shape the plan, and review the output together before starting implementation. Caroline Sheng will drive this process.1

Context for Avatica Phase 3:

* PRs
    * https://github.com/forcedotcom/datacloud-jdbc/pull/136
* Google Doc
    * https://docs.google.com/document/d/1HWbVSPQHjTjgYi_xDhrCQ90DyeXF3AAGQw2xBlbW9Dk/edit?tab=t.sit1ned074c6#heading=h.441irkmmxjci
* Gus item
    * https://gus.lightning.force.com/lightning/r/ADM_Sprint__c/a0lEE000001mUWLYA2/view
        * Copy-pasted as follows:


Work
[Path to V1] 1.3 Final Avatica Cleanup and Integration (Avatica Removal Phase 3)
Work ID

W-19655253
Status

Triaged
Record Type

User Story
Team

CDP Query Service (US)
Sprint

2026.03b-CDP Query Service (US)
Assigned To

Caroline Sheng
Caroline Sheng
Tabs
Tabs
Details
Related
Expanded Feed
Security
Quip
Subject
[Path to V1] 1.3 Final Avatica Cleanup and Integration (Avatica Removal Phase 3)
Description
Avatica removal focusing on data type conversions
Final cleanup and integration work to complete Avatica removal.
This will be needed for becoming V1.



Context: Final phase of Avatica removal including cleanup, integration testing, and ensuring all dependencies are properly removed.
Implementation Guide: Avatica Removal Guide

