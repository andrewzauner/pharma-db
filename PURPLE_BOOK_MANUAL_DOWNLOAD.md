# Purple Book Manual Download Instructions

The FDA Purple Book website (https://purplebooksearch.fda.gov/) is a JavaScript-based application that doesn't provide direct download URLs. You'll need to download the data manually.

## Quick Steps

1. **Visit the Purple Book website:**
   - Go to: https://purplebooksearch.fda.gov/

2. **Find the Export/Download button:**
   - Look for buttons labeled "Export", "Download", "Export to CSV", or "Export to Excel"
   - These are usually in the top toolbar or in a menu

3. **Download the file:**
   - Click the export button
   - Choose CSV or XLSX format
   - Save the file to your computer

4. **Place it in the raw directory:**
   ```bash
   # Copy the downloaded file to:
   data/purple_book_raw/purplebook_data.csv
   # OR
   data/purple_book_raw/purplebook_data.xlsx
   ```

5. **Run the ingest script:**
   ```bash
   python purple_book_ingest.py
   ```

   The script will automatically detect and process the file.

## Alternative: Use --local Flag

If you downloaded the file to a different location:

```bash
python purple_book_ingest.py --local "C:\path\to\your\purplebook.csv"
```

## What the Script Does

Once you provide the file (either by placing it in `data/purple_book_raw/` or using `--local`), the script will:

1. Read the CSV/XLSX file
2. Parse and normalize the data
3. Create output files:
   - `pb_dim_product.csv`
   - `pb_dim_biosimilar.csv`
   - `pb_fact_exclusivity.csv`
   - `pb_dim_application.csv`
4. These files can then be loaded to PostgreSQL

## Troubleshooting

**File not found:**
- Make sure the file is in `data/purple_book_raw/` directory
- Check the file name (should end in .csv or .xlsx)
- Use `--local` with the full path if needed

**Empty output:**
- Check that the downloaded file has data (open it in Excel/notepad)
- Run with `--debug` to see what's happening
- The file might be in a different format than expected

**Can't find Export button:**
- The website interface may have changed
- Try looking in different menus or toolbars
- Check if there's a "Data" or "Tools" menu
- Some sites require you to view/search data first before export is available

## Automation (Advanced)

If you want to automate this, you can:

1. **Use Selenium** (requires setup):
   ```bash
   pip install selenium
   # Also need ChromeDriver
   ```
   The script will attempt browser automation if Selenium is available.

2. **Use browser extensions:**
   - Some browser extensions can automate downloads
   - Set up a scheduled task to run the download

3. **Check for API access:**
   - The FDA may provide API access in the future
   - Check https://open.fda.gov/ for updates

## Current Status

As of now, manual download is the most reliable method. The script is optimized to work seamlessly once you provide the downloaded file.


