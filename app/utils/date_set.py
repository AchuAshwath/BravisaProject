import datetime

def get_run_date():
    #Adjust the timedelta value accordingly to run reports for backdate. For backdates, values inside timedelta should
    #be negative count of the difference between current date and backdate.
    #By default, value is set to 0 to run reports for current date.
    run_date = datetime.date.today() + datetime.timedelta(-104)
    return run_date

def get_one_day_back():
    run_date = get_run_date()
    one_day_back = run_date + datetime.timedelta(-1)
    return one_day_back