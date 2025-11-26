# Purple Book Download Troubleshooting

## Recent Improvements

I've enhanced the Purple Book download logic with:

1. **Better Error Handling**: Files are verified after download to ensure they're valid
2. **Improved Logging**: More detailed logs show which URLs are being tried
3. **File Verification**: Downloads are checked for:
   - File size (must be > 512 bytes)
   - File type (rejects HTML error pages)
   - File existence
4. **Multiple URL Patterns**: Tries various URL formats the FDA might use
5. **Debug Mode**: Use `--debug` flag to see detailed information

## How to Use

### Normal Run
```bash
python purple_book_ingest.py
```

### Debug Mode (Recommended for Troubleshooting)
```bash
python purple_book_ingest.py --debug
```

This will show:
- All URLs being tried
- Download attempts and results
- File verification details
- Files found in the raw directory

### Manual Download
If automatic download fails, you can:

1. **Use a direct URL:**
   ```bash
   python purple_book_ingest.py --url "https://purplebooksearch.fda.gov/downloads/files/..."
   ```

2. **Use a local file:**
   ```bash
   python purple_book_ingest.py --local "path/to/purplebook.csv"
   ```

3. **Place file manually:**
   - Download the Purple Book data manually from https://purplebooksearch.fda.gov/
   - Place the CSV/XLSX file in `data/purple_book_raw/`
   - Run the script again

## Common Issues

### Issue: "No files to parse"

**Possible causes:**
1. All download URLs returned 404 (FDA changed URL structure)
2. Network/firewall blocking downloads
3. Files downloaded but are HTML error pages

**Solutions:**
1. Run with `--debug` to see which URLs are being tried
2. Check if you can access https://purplebooksearch.fda.gov/ in a browser
3. Manually download the file and use `--local` option
4. Check the `data/purple_book_raw/` directory for any downloaded files

### Issue: Files download but are empty/HTML

The script now automatically detects and rejects:
- Files smaller than 512 bytes
- HTML error pages
- Invalid file types

If this happens, the script will try the next URL automatically.

### Issue: Timeout errors

The timeout has been increased to 60 seconds. If you still get timeouts:
1. Check your internet connection
2. Try downloading manually to verify the URL works
3. Use `--local` with a manually downloaded file

## Verifying Downloads

After running the script, check:

1. **Raw files directory:**
   ```bash
   ls -lh data/purple_book_raw/
   ```

2. **Output files:**
   ```bash
   ls -lh data/pb_*.csv
   ```

3. **Check file sizes:**
   - Raw files should be > 1KB (usually much larger)
   - Output CSVs should have data rows

## Getting Help

If downloads still fail:

1. Run with `--debug` and save the output
2. Check the FDA website manually: https://purplebooksearch.fda.gov/
3. Look for a "Download" or "Export" button on the website
4. Note the actual download URL from the browser
5. Use that URL with `--url` option

## Alternative: Manual Download Workflow

If automatic download continues to fail:

1. Visit https://purplebooksearch.fda.gov/
2. Find and click the download/export button
3. Save the file (CSV or XLSX)
4. Place it in `data/purple_book_raw/`
5. Run: `python purple_book_ingest.py`

The script will detect the file and process it automatically.

