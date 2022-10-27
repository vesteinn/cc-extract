# cc-extract

A simple tool for extracting low resource corpora from the Common Crawl

Download the fasttext language classification model into the root directory

```bash
wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
```

The same script needs to be ran in a few steps.

```
usage: extract_by_tld.py [-h] [--search SEARCH]
                         [--search_dir SEARCH_DIR]
                         [--fetch FETCH] [--warc_out WARC_OUT]
                         [--n_proc N_PROC] [--extract EXTRACT]
                         [--extract_out EXTRACT_OUT]
                         [--stoplist_lang STOPLIST_LANG]
                         [--fasttext_lang FASTTEXT_LANG]
                         [--fasttext_lang_ignore FASTTEXT_LANG_IGNORE]

optional arguments:
  -h, --help            show this help message and exit
  --search SEARCH
  --search_dir SEARCH_DIR
  --fetch FETCH
  --warc_out WARC_OUT
  --n_proc N_PROC
  --extract EXTRACT
  --extract_out EXTRACT_OUT
  --stoplist_lang STOPLIST_LANG
  --fasttext_lang FASTTEXT_LANG
  --fasttext_lang_ignore FASTTEXT_LANG_IGNORE
```
