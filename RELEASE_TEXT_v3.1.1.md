Release v3.1.1 Latest
@gbsDojo gbsDojo released this Apr 22, 2026
· release updates to main since this release
v3.1.1

## 🚀 What's Changed

### 📘 Description:
This release introduces version 3.1.1 of the DOJO Beam Transforms library, focused on release-readiness updates, dependency/version alignment, and documentation refresh. It updates README guidance, modernizes compatibility targets, and improves reliability in key pipeline components.

---

### 🧱 General Refactoring:
- Updated and standardized release documentation for GitHub and PyPI.
- Aligned version references across project modules and release metadata.
- Refreshed usage examples to match currently available transforms and signatures.
- Improved consistency of release configuration and publishing workflow output references.

---

### 🧩 Modules and Components Overview:

**`pipeline_components/`**

- AbstractSchemaProvider, AbstractStructuredTransform, AutoRunPipeline, PipelineOptionsProvider, input_file

**`pipeline_components.data_cleaning/`**

- DropColumns, DropDuplicates, HandleNaNValues, KeepColumns, RemoveAccents, RenameColumns, ReplaceRegex, ReplaceValues, TrimValues
pipeline_components.data_enrichment/

**`pipeline_components.data_enrichment/`**
- AddPeriodColumn, ColumnValueAssignment, ColumnsToDate, ColumnsToDecimal, ColumnsToFloat, ColumnsToInteger, ColumnsToLowerCase, ColumnsToString, ColumnsToUpperCase, GenericArithmeticOperation, GenericDeriveCondition, GenericDeriveConditionComplex, Join, MergeColumns, OrderFieldsByArrowSchema, OrderFieldsBySchema, ReplaceMissingValues, SplitColumn

---

### ✅ Test Coverage & Automation:
- Test suite written with `pytest` covering all transformations.
- Current test run status: 100 passed.
- GitHub Actions Docker uploader workflow output variable reference corrected (`version`).

---

### 🧹 Clean-Up and Removals:
- Deprecated README sections and outdated examples were removed/replaced.
- Legacy release-version references in docs were updated to v3.1.1.
- Dependency list in requirements was simplified and aligned with the package release matrix.

---

### 🌟 Enhancements:
- README fully updated with current installation instructions and practical examples.
- Release text prepared for direct use in GitHub Releases and PyPI notes.
- PipelineOptionsProvider default container version updated to 3.1.1.
- DojoBeamTransformVersion enum extended with V3_1_0.
- Improved error behavior in header application flow for mismatched schema length.
- DropDuplicates reliability improved for state handling in processing.

---

### 📌 Semantic Versioning Notice (v3.1.1)
- This release follows semver.org and is a minor release focused on compatibility updates, release standardization, and reliability/documentation improvements. Existing pipelines should remain compatible, with improved defaults and clearer guidance.

---

### 🧪 Compatible Python Versions:
Python 3.12 ✅

---

### 🔧 Dependency Versions for Release 3.1.1:
Apache Beam SDK:
apache-beam[dataframe,gcp,interactive] == 2.72.0
Core Dependencies:
pandas == 2.1.1
numpy == 1.26.3
pytz == 2025.2
openpyxl == 3.1.5

---

### ✅ Best Practices & Project Guidelines:
Commit and Branch Naming:
Follows Conventional Commits (e.g., feat:, fix:, refact:, docs:, chore:) for structured history.
Feature branches use prefix naming like feature/, fix/, chore/, or release/.
Versioning:
Uses Semantic Versioning (SemVer) to signal breaking changes, new features, and patches clearly.
