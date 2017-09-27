

def datepart(date):
    if len(date)==10:
        y = date[:4]
        m = date[5:7]
        d = date[8:10]
        return y, m, d
    elif len(date)==8:
        y = date[:4]
        m = date[4:6]
        d = date[6:8]
        return y, m, d
    else:
        raise ValueError('invalid date:' + str(date))