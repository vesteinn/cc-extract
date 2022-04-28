import os
import datetime
import requests
import argparse
import time
import json
import subprocess
import tqdm
import justext
import fasttext
from multiprocessing import Pool


CC_IDX_URL = "https://index.commoncrawl.org/"
CC_IDX_API_HEAD = "https://index.commoncrawl.org/"

JUSTEXT_CONFIG = {
    "length_low": 40,
    "length_high": 250,
    "stopwords_high": 0.1,
    "stopwords_low": 0.3,
    "max_link_density": 0.2,
    "max_heading_distance": 3,
    "no_headings": True
}


try:
    lid_model = fasttext.load_model("lid.176.bin") 
except:
    print("Fasttext model lid.176.bin not found, language filter will not work.")


def get_idx_urls():
    r = requests.get(CC_IDX_URL)
    idxs_d = [l for l in r.text.split("\n") if '<a href="/CC-MAIN' in l]
    idxs = [t.split('"/')[1].split('"')[0] for t in idxs_d]
    return [f"{CC_IDX_API_HEAD}{idx}" for idx in idxs]


def search_all(query, outdir):
    api_urls = get_idx_urls()
    for url in api_urls:
        print(f"Fetching query: '{query}' from dump {url}.")
        not_done = True
        while not_done:
            r = requests.get(f"{url}-index?url={query}&output=json")
            if "Internal Error: 503" not in r.text:
                not_done = False
            else:
                time.sleep(5000)
        clean_query = query.replace("*","").replace(".", "")
        clean_url = url.split("/")[-1]
        with open(f"{outdir}/{clean_query}_{clean_url}.json", "w") as outf:
            outf.writelines(r.text)


def get_json_data(tj):
    end = int(tj["offset"]) + int(tj["length"])
    with open(os.devnull, 'w') as devnull:
        ps = subprocess.Popen(['curl', '-H', 'Range: bytes={}-{}'.format(tj['offset'], end), 'https://commoncrawl.s3.amazonaws.com/' + tj['filename']], stdout=subprocess.PIPE, stderr=devnull)
        proc = subprocess.Popen(['gunzip'], stdin=ps.stdout, stdout=subprocess.PIPE, stderr=devnull)
        stdout, _ = proc.communicate()
        ps.wait()
        return stdout


def get_data_from_search_file(in_n_out):
    infile, outfile = in_n_out
    with open(infile) as infh, open(outfile, "w") as outfh:
        for line in infh.readlines():
            try:
                line_json = json.loads(line)
            except:
                continue
            data = get_json_data(line_json)
            outfh.writelines(str(data, 'ISO-8859-1'))


def read_block(file_stream, break_str=None, start=None, end_str='\n'):
    lines = []
    if start is None:
        line = file_stream.readline()
        try:
            line = line.encode('iso-8859-1').decode('utf-8')    
        except:
            pass
    else:
        line = start
    lines.append(line)

    if break_str is not None and line[:len(break_str)] != break_str:
        raise Exception('Break str not found')

    while line!= '' and line != end_str:
        line = file_stream.readline()
        try:
            line = line.encode('iso-8859-1').decode('utf-8')
            lines.append(line)
        except:
            line = "="
    return file_stream, lines


def read_warc_head(file_stream, start=None):
    return read_block(file_stream, 'WARC', start)


def read_header(file_stream):
    return read_block(file_stream, 'HTTP')


def read_html(file_stream, start=None):
    return read_block(file_stream, None, start, end_str="WARC/1.0\n")


def xtr(str_list, s):
    text = []
    try:
        paragraphs = justext.justext(
            "".join(str_list),
            s,
            **JUSTEXT_CONFIG
    )
    except Exception:
       return text
    for paragraph in paragraphs:
        if not paragraph.is_boilerplate:
            text.append(paragraph.text)
    return text



def parse_file(in_n_out):
    file_name, out_file, stop_list, lang, lang_ignore = in_n_out
    s = justext.get_stoplist(stop_list)

    with open(file_name, errors='ignore') as f, open(out_file, "w") as of, open(out_file + ".txt", "w") as otf:
        cont = None
        while True:
            try:
                f, _wh = read_warc_head(f, start=cont)
            except:
                return
            f, _h = read_header(f)
            f, _p = read_html(f)
            cont = _p[-1]
            text = xtr(_p[:-1], s)
            if lang or lang_ignore:
                ret_text = []
                for pg in text:
                    for t in pg.split("\n"):
                        t_lang = lid_model.predict(t)[0][0].split("__")[-1]
                        if lang and lang == t_lang:
                            # include filter
                            ret_text.append(t + "\n")
                        elif lang_ignore and lang_ignore != t_lang:
                            # ignore filter
                            ret_text.append(t + "\n")
                        elif not lang and not lang_ignore:
                            # no filter
                            ret_text.append(t + "\n")
            else:
                ret_text = text

            if not ret_text:
                continue

            of.writelines(_wh)
            of.writelines('\n')
            of.writelines(_h)
            of.writelines('\n')
            of.writelines(ret_text)
            of.writelines('\n')

            otf.writelines(ret_text)
            otf.writelines('\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", type=str)
    parser.add_argument("--search_dir", type=str)
    parser.add_argument("--fetch", type=bool, default=False)
    parser.add_argument("--warc_out", type=str)
    parser.add_argument("--n_proc", type=int, default=1)
    parser.add_argument("--extract", type=bool, default=False)
    parser.add_argument("--extract_out", type=str)
    parser.add_argument("--stoplist_lang", type=str)
    parser.add_argument("--fasttext_lang", type=str)
    parser.add_argument("--fasttext_lang_ignore", type=str)
    args = parser.parse_args()
 
    if args.search:
        os.mkdir(args.search_dir)
        search_all(args.search, args.search_dir)
        print("Done, you can now use the --fetch option")
    elif args.fetch:
        if not args.search_dir:
            print("--search_dir not set, should point to folder with jsonl files")
        files = os.listdir(args.search_dir)
        os.mkdir(args.warc_out)
        for i in range(0, len(files), args.n_proc):
            pool_args = [] 
            file_names = files[i:i + args.n_proc]
            for file_name in file_names:
                outfile_name = args.warc_out + "/" + file_name.split("/")[-1]
                pool_args.append((f"{args.search_dir}/{file_name}", outfile_name))
            outfile_names = [p[1] for p in pool_args]
            now = datetime.datetime.now()
            print(f"{now}: Now fetching {file_names}, writing to {outfile_names}.")
            with Pool(args.n_proc) as p:
                p.map(get_data_from_search_file, pool_args)
    elif args.extract:
        if not args.warc_out:
            print("--warc_out not set, should contain warc files")
        os.mkdir(args.extract_out)
        files = os.listdir(args.warc_out)
        for i in range(0, len(files), args.n_proc):
            pool_args = [] 
            file_names = files[i:i + args.n_proc]
            for file_name in file_names:
                outfile_name = args.extract_out + "/" + file_name.split("/")[-1]
                pool_args.append(
                    (f"{args.warc_out}/{file_name}",
                     outfile_name,
                     args.stoplist_lang,
                     args.fasttext_lang,
                     args.fasttext_lang_ignore))
            outfile_names = [p[1] for p in pool_args]
            now = datetime.datetime.now()
            print(f"{now}: Now extracting text from {file_names}, writing to {outfile_names}.")
            with Pool(args.n_proc) as p:
                p.map(parse_file, pool_args)


if __name__ == "__main__":
    main()
