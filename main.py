import config
import telebot
import mysql.connector
import datetime
import sys
import pandas as pd


class Diagram_table:
    def __init__(self, table_name,year, month,day,type_name,amount_of_brands,amount_of_units,predicted_market_volume,dynamic_compared_to_previous_month,dynamic_compared_to_previous_year):
        self.table_name = table_name
        self.year = year
        self.month = month
        self.day = day
        self.type_name = type_name
        self.amount_of_brands = amount_of_brands
        self.amount_of_units = amount_of_units
        self.predicted_market_volume = predicted_market_volume
        self.dynamic_compared_to_previous_month = dynamic_compared_to_previous_month
        self.dynamic_compared_to_previous_year = dynamic_compared_to_previous_year

    table_df = pd.DataFrame()

    def Get_df_of_table(self,cursor,entered_year,entered_month,entered_day):
        sql_query = "select {0},{1},{2},{3},{4},{5} from {6} where ({7}={8})&({9}={10})&({11}='{12}');".format(self.type_name,self.amount_of_brands,self.amount_of_units,self.predicted_market_volume,self.dynamic_compared_to_previous_month,self.dynamic_compared_to_previous_year,self.table_name,self.year,entered_year,self.month,entered_month,self.day,entered_day)
        cursor.execute(sql_query)
        self.table_df = pd.DataFrame(data=cursor.fetchall(),columns = ['Type','Amount_of_brands' , 'Amount_of_units', 'Predicted_market_volume', 'Dynamic_compared_to_previous_month','dynamic_compared_to_previous_year'])


def Create_result_message(First_sentence,count_of_markets,df,entered_year,entered_month, entered_period):
    #!!!!!!!!!!!!!!!!!!!!!!
    amount_of_companies = df[df["Type"]=="Легковые авто"]["Amount_of_brands"].tolist()
    msg = First_sentence + "результаты продаж за {0} месяц {1} на {2} дней. Внесены показатели {3} компаний.\n\nПредварительный прогноз:\n".format(entered_month,entered_year,entered_period,amount_of_companies[0])
    for i in range(count_of_markets):
        msg = msg + "Рынок {0} - {1} шт\n".format(df.loc[i][0],df.loc[i][3])
    msg = msg + "\nДинамика:\n"
    for i in range(count_of_markets):
        msg = msg + "Рынок {0}: {1}% с предидущим месяцем, {2}% с предидущим годом\n".format(df.loc[i][0],df.loc[i][4],df.loc[i][5])

    return msg

def Create_remind_message(entered_year,entered_month, entered_period):
    if entered_period==1:
        if month==1: entered_year = entered_year -1
        msg = "НАПОМИНАНИЕ: Сегодня можно вносить данные продаж полный {0} месяц {1}.".format(entered_month-1,entered_year)
    else:
        msg = "НАПОМИНАНИЕ: Сегодня можно вносить данные продаж за {0} дней {1} месяца {2}.".format(entered_period,entered_month,entered_year)
    return msg


def logging(filename,message):
    file = open(filename, "a")
    file.write(str(datetime.datetime.now())+": "+message+"\n")
    file.close()


if __name__ == "__main__":
    bot = telebot.TeleBot(config.token)
    host = config.host
    user = config.user
    password = config.password
    database = config.database
    logfile_name = config.logfile_name
    CHANNEL_NAME = config.CHANNEL_NAME
    year = datetime.datetime.today().year
    period = (datetime.datetime.today().day//7)*7
    month = datetime.datetime.today().month
    #month = 1

    try:
        if (len(sys.argv)!=2):raise Exception('Wrong quantity of argv')
        else:
            what_bot_must_do = sys.argv[1]
    except Exception as ex:
        logging(logfile_name,str(ex))
        quit()

    # what_bot_must_do = "remind"
    # period = "14 дней"

    try:
        Autocon_database = mysql.connector.connect(
          host=host,
          user=user,
          passwd=password,
          database=database
        )
        cursor = Autocon_database.cursor()
    except Exception as err:
        logging(logfile_name, "Problem with database connection"+str(err))
        quit()


    try:
        if (what_bot_must_do == "remind")|(what_bot_must_do == "Remind"):
            message = Create_remind_message(year, month, period)
            bot.send_message(CHANNEL_NAME, message, parse_mode='Markdown')

        elif (what_bot_must_do == "operativ")|(what_bot_must_do == "Operativ"):
            diagram__table_obj = Diagram_table("Diagram","year","month","period","type_name","amount_of_brands","amount_of_units","pridicted_market_volume","dynamic_compared_to_previous_month","dynamic_compared_to_previous_year")
            diagram__table_obj.Get_df_of_table(cursor=cursor,entered_year=year,entered_month=month,entered_day=period)
            if diagram__table_obj.table_df.empty is True: raise Exception('No data for this month')
            message = Create_result_message(First_sentence="Оперативные ", count_of_markets=len(diagram__table_obj.table_df), df=diagram__table_obj.table_df, entered_year=year, entered_month=month, entered_period=period)
            bot.send_message(CHANNEL_NAME,message, parse_mode='Markdown')

        elif (what_bot_must_do == "update")|(what_bot_must_do == "Update"):
            diagram__table_obj = Diagram_table("Diagram", "year", "month", "period", "type_name", "amount_of_brands",
                                               "amount_of_units", "pridicted_market_volume",
                                               "dynamic_compared_to_previous_month", "dynamic_compared_to_previous_year")
            diagram__table_obj.Get_df_of_table(cursor=cursor, entered_year=year, entered_month=month, entered_day=period)
            if diagram__table_obj.table_df.empty is True: raise Exception('No data for this month')
            message = Create_result_message(First_sentence="Уточненные ",
                                            count_of_markets=len(diagram__table_obj.table_df),
                                            df=diagram__table_obj.table_df, entered_year=year, entered_month=month,
                                            entered_period=period)
            bot.send_message(CHANNEL_NAME, message, parse_mode='Markdown')

        else: raise Exception('Wrong argument')
    except Exception as err:
        logging(logfile_name, str(err))
    finally:
        Autocon_database.close()
        quit()




