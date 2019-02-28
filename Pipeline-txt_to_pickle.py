import os
import pandas as pd
import pickle


def generate_pickle_from_txts(ticker, period):
    data_path = 'C://Users//Alexis//PycharmProjects//StockWatch//FullData//'
    files = []
    for file in os.listdir(data_path):
        if '_' + str(period) in file:
            files.append(data_path + file)

    data = pd.DataFrame(columns=['timestamp', 'close'])
    for file in files:
        # print('{}/{}'.format(files.index(file) + 1, len(files)))
        with open(file, 'r') as f:
            end = False
            while not end:
                line = f.readline().split(',')
                if len(line[0]):
                    if line[0] == ticker:
                        data = data.append(pd.DataFrame([[line[1], float(line[5])]], columns=['timestamp', 'close']))
                else:
                    end = True

    data = data.set_index('timestamp', drop=True, inplace=False)
    pickle.dump(data, open('Data/{}_{}.pickle'.format(ticker, period), 'wb'))


if __name__ == '__main__':
    tickers = ['MU', 'QQQ', 'AAPL', 'INTC', 'MSFT', 'CMCSA', 'CSCO', 'FB', 'SIRI', 'IQ', 'HMNY', 'NVDA', 'JD', 'TQQQ', 'AMAT', 'QCOM', 'ZNGA', 'SQQQ', 'FOXA', 'AABA', 'HBAN', 'SBUX', 'EBAY', 'GRPN', 'TLT', 'NFLX', 'GILD', 'PYPL', 'MDLZ', 'CZR', 'MRVL', 'ROKU', 'TLRY', 'CSX', 'SYMC', 'PDD', 'TSLA', 'ATVI', 'NVAX', 'ON', 'MAT', 'AAL', 'ENDP', 'FITB', 'CY', 'OCLR', 'ODP', 'WBA', 'CELG', 'MYL', 'JBLU', 'ERIC', 'TXN', 'PLUG', 'SGYP', 'BILI', 'OPK', 'DBX', 'MOMO', 'STNE', 'FEYE', 'CTRP', 'STX', 'VOD', 'NXPI', 'VIAB', 'AMZN', 'AGNC', 'DISCA', 'PEP', 'GPRO', 'WDC', 'FLEX', 'KHC', 'ESRX', 'HIMX', 'UAL', 'TMUS', 'EXEL', 'CTSH', 'GPOR', 'VEON', 'BBBY', 'AKER', 'TVIX', 'PTEN', 'AMRN', 'MNKD', 'GERN', 'CRON', 'MLCO', 'FOX', 'ARRY', 'GLUU', 'AVGO', 'AMGN', 'EA', 'WEN', 'FNSR', 'UPL', 'SLM', 'NTNX', 'YNDX', 'LBTYK', 'IBB', 'PBCT', 'QRTEA', 'GT', 'FTR', 'BPR', 'NAVI', 'NTAP', 'DLTR', 'XEL', 'ASNA', 'FOLD', 'URBN', 'DISCK', 'ADI', 'TXMD', 'TRIP', 'ROST', 'BIDU', 'STLD', 'FAST', 'MCHP', 'ADBE', 'PFF', 'NUAN', 'SABR', 'ETFC', 'HZNP', 'SPWR', 'IMGN', 'DISH', 'LRCX', 'CRZO', 'COST', 'AKRX', 'MNST', 'EMB', 'IMMU', 'XLNX', 'NBEV', 'AMTD', 'VIAV', 'WYNN', 'MCHI', 'IEF', 'HOLX', 'USLV', 'ADP', 'ZION', 'ISBC', 'AVEO', 'FSLR', 'CAR', 'EXAS', 'SONO', 'CERN', 'DRYS', 'MAR', 'SFM', 'QTT', 'CENX', 'NWSA', 'LULU', 'PSEC', 'INFO', 'HDS', 'NKTR', 'RIOT', 'CDNS', 'MIK', 'ACAD', 'MXIM', 'ETSY', 'MDXG', 'LBTYA', 'CYTR', 'ADSK', 'MDRX', 'ALXN', 'GNTX', 'BND', 'VLY', 'ACWI', 'WB', 'FOSL', 'PAYX', 'AUPH', 'PCAR', 'AKAM', 'SWKS', 'XRAY', 'MNGA', 'LKQ', 'TTWO', 'ARCC', 'UNIT']
    period = 5
    for ticker in tickers[100:]:
        print('{}/{}'.format(tickers.index(ticker) + 1, len(tickers)))
        generate_pickle_from_txts(ticker, period)
