import datetime

def get_holidays():
    holidays = ['February 21,2020', 'March 10,2020', 'April 02,2020', 'April 06,2020',
                        'April 10,2020', 'April 14,2020', 'May 01,2020', 'May 25,2020',
                        'October 02,2020', 'November 16,2020', 'November 30,2020', 'December 25,2020',
                        'January 26,2021', 'March 11,2021', 'March 29,2021', 'April 02,2021', 
                        'April 14,2021', 'April 21,2021', 'May 13,2021', 'July 21,2021', 
                        'August 19,2021', 'September 10,2021', 'October 15,2021', 'November 04,2021', 
                        'November 05,2021', 'November 19,2021']
    for i in range (len(holidays)):
        date = datetime.datetime.strptime(holidays[i], '%B %d,%Y').strftime('%Y-%m-%d').split("-")
        holidays[i] = datetime.date(int(date[0]), int(date[1]), int(date[2]))
    return holidays

holidays = get_holidays()
def is_holiday(curr_date):
    try:
        if (curr_date in holidays):
            # print("Ran normally")
            return True
        else:
            # print("Ran normally")
            return False
    except Exception as e:
        print(e)