import re

"""
Python script used to clean the csv files for Spark processing,
due to Spark failing to read the original files
"""

list_regex = [
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "(\"[[].*?[]]\",?)|([[]'.*?'[]],?)",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "[0-9]*,?",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "[01],?",
    ".*?,",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "[0-9]*,",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "-?[0-9]*[.][0-9]*(e-[0-9]*)*,?",
    "[01],?",
    "(\"?.*((\"\").*(\"\").*)+?\",)|(\"?.*?\",)|(.*?,)",
    "[0-9]*,?",
    ".*?,",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?",
    "[0-9]*"
]


def getLines(input_file):
    with open(input_file, "r") as file_:
        while True:
            out_line = ""
            line = file_.readline()
            if not line:
                break
            sub_line = line
            for index in range(len(list_regex)):
                find_regex = re.search(list_regex[index], sub_line)
                if find_regex:
                    start_index = find_regex.start()
                    end_index = find_regex.end()
                    if index == len(list_regex) - 1:
                        out_line = out_line + sub_line[start_index:end_index].replace(",", ";")
                    else:
                        out_line = out_line + sub_line[start_index:end_index - 1].replace(",", ";") + ","
                    # print(out_line)
                    sub_line = sub_line[end_index:]
                else:
                    print("Line does not match ->", line)
                    out_line = line[:-1]
                    break
            # print(line)
            yield out_line


if __name__ == '__main__':
    file_path = "src/main/resources/data/csv/data.csv"
    lines = getLines(file_path)
    file_out = "src/main/resources/data/csv/data_.csv"
    for line_cleaned in lines:
        with open(file_out, "a") as out_file:
            out_file.write(line_cleaned)
            out_file.write("\n")
