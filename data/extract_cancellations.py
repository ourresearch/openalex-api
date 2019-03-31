import csv
import re


'''
test cases for the dates extraction
v.18(2007)-
2006-
v.1(1999)-v.4(2003).
v.4:no.3(2005:Dec.)-v.7:no.1(2009:Jan.).
v.2(2001)-v.8:no.3(2007:May/June).
v.28(1996)-v.33:no.4(2001:Nov./Dec.).
v.3:no.2(2008)-v.6(2011).
v.1-2(2013)-

'''

dates = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}


def extract_date(str):
    if not str:
        return ""

    # extract start date
    ret = ""
    regex = r"\d\d\d\d:\w\w\w|\d\d\d\d"
    m = re.search(regex, str)
    if m:
        my_date = m.group()
        my_date = my_date.lower()
        my_date = my_date.replace(":", "-")
        for month_string, month_iso in dates.iteritems():
            my_date = my_date.replace(month_string, month_iso)
        ret = my_date

    return ret



def split_dates(str):
    segments = str.split("-")
    date_segments =  [x for x in segments if re.search("\d\d\d\d", x)]

    if len(date_segments) == 0:
        return ["", ""]
    elif len(date_segments) == 1:
        return date_segments + [""]
    else:
        return date_segments








def journal_with_dates(journal_row):
    all_issns = (journal_row[0] + ";" + journal_row[1]).split(";")
    all_issns = "|".join([x for x in all_issns if x])

    date_halves = split_dates(journal_row[3])
    start_date = extract_date(date_halves[0])
    end_date = extract_date(date_halves[1])

    return [
        all_issns,
        start_date,
        end_date
    ]


def get_and_store():
    output_rows = []
    with open('cancellations-input.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        journal_arr = []
        for journal_row in csv_reader:
            journal_arr.append(journal_row)

        for journal_row in journal_arr[1:]:
            new_journal_row = journal_with_dates(journal_row)
            output_rows.append(new_journal_row)


    with open('cancellations-output.csv', mode='w') as f:
        writer = csv.writer(f)
        for row in output_rows:
            # writer.writerow(row)

            # normalize. multiple rows per journal, but just one row per ISSN
            for issn in row[0].split("|"):
                writer.writerow([issn, row[1], row[2]])


if __name__ == "__main__":
    print "i am running"
    get_and_store()
