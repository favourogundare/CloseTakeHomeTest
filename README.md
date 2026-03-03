# Close CRM Import Script

This script imports companies/contacts from a CSV into Close CRM, searches for leads founded within a date range, and generates a state‑segmented report.

---

## How Invalid Data Was Eliminated

Before import, each row is validated:

- **Company name** – required; rows without a name are skipped.
- **Email addresses** – checked for `@` and a valid domain; malformed addresses are discarded.
- **Phone numbers** – cleaned of non‑digit characters; only numbers with ≥4 digits are kept.
- **Contact names** – normalized to proper case; not required if email/phone is valid.
- **Founding date** – parsed from `DD.MM.YYYY`; invalid dates are left blank.
- **Revenue** – cleaned of `$` and commas; both `$1,234,567` and `1234567` formats are accepted.
- **US State** – only valid state names are kept; others are blank.

---

## How All Leads in a Date Range Were Found

1. Fetch all leads from Close via API.
2. Check each lead’s “Company Founded” date.
3. Keep only leads whose founding date falls between the user‑provided start and end dates.
4. Return the filtered list with revenue and state information.

---

## How Leads Were Segmented by State and the Top‑Revenue Lead Found

- Leads are grouped by US state.
- For each state:
  - Count total leads.
  - Find the lead with the highest revenue.
  - Sum total revenue.
  - Calculate median revenue.
- Results are written to a CSV file.

---

## How to Run the Script

### Dependencies
- Python Version 3.7+
- Install `requests` library:
- Install `dotenv` library:
  ```bash
  pip install requests
  pip install dotenv

  ```
- All other modules (`csv`, `re`, `datetime`, `statistics`, `argparse`) are built‑in.

### Setup
1. Save the script as `import.py`.
2. Create a `.env` file in the same folder with your Close API key:
   ```
   CLOSE_API_KEY=api_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
   ```

### Commands
Three actions are available:

| Action | Description |
|--------|-------------|
| `import` | Import CSV data into Close. |
| `report` | Query Close for leads in a date range and generate state report. |
| `all`   | Import CSV **and** generate the report. |

#### Examples

**Import only:**
```bash
python import.py import --csv "path/to/input.csv"
```

**Report only:**
```bash
python import.py report --start-date 1960-01-01 --end-date 2000-12-31 --output "state_report.csv"
```

**Do everything:**
```bash
python import.py all --csv "import.csv" --start-date 1960-01-01 --end-date 2000-12-31 --output "report.csv"
```

### Input CSV Columns
The CSV must contain these exact headers:
- `Company`
- `Contact Name`
- `Contact Emails`
- `Contact Phones`
- `custom.Company Founded` (DD.MM.YYYY)
- `custom.Company Revenue`
- `Company US State`

### Output Report Columns
- `US State`
- `Total number of leads`
- `The lead with most revenue`
- `Total revenue`
- `Median revenue`