import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logging.basicConfig(filename= "logs/logFile.log", filemode='w', level=logging.DEBUG)
# logging.warning('This will get logged to a file')
logging.info("Projection started.")

class SalesDataProjection:
    def __init__(self):
        self.resultSet = []
        self.TotalRows = []
        try:
            logging.info("Sales  Data Consolidation")
            self.SalesData2003 = pd.read_json("data/SalesData_2003.json")
            self.SalesData2004 = pd.read_json("data/SalesData_2004.json")
            self.SalesData2005 = pd.read_json("data/SalesData_2005.json")
            
            #combine 3 dataframes into 1
            self.SalesDataConsolidated = pd.concat([self.SalesData2003,self.SalesData2004,self.SalesData2005])
            self.TotalRows = len(self.SalesDataConsolidated)
            self.resultSet = self.SalesDataConsolidated

            msg = "Sales Data Consolidated. TotalRows =", str(self.TotalRows)
            logging.info(msg)
        except Exception as exInit:
            msg = exInit
            logging.info(msg)
            print(msg)
        finally:
            msg = "Sales Data Consolidated. TotalRows =", str(self.TotalRows)
            logging.info(msg)
            print(msg)

    def downloadExcel(self, df, filename):
        try:
            # saving the excel
            df.to_excel(filename)
            msg = filename + " saved successfully."
            logging.info(msg)
            print(msg)
        except Exception as exdownloadExcel:
            msg = exdownloadExcel
            logging.info(msg)
            print(msg)
        finally:
            msg = "Finally of downloadExcel"
            logging.info(msg)
            print(msg)
    
    def addColumnMSRPSales(self):
        try:
            self.SalesDataConsolidated["MSRPSales"] = self.SalesDataConsolidated["MSRP"] * self.SalesDataConsolidated["QUANTITYORDERED"]
        except Exception as exaddColumnMSRPSales:
            msg = "exaddColumnMSRPSales"
            logging.info(msg)
            print(msg)
        finally:
            msg = "MSRPSales added."
            logging.info(msg)
            print(msg)

    def saveSalesDataToParquet(self):
        try:
            # Convert DataFrame to Arrow Table
            tableSalesData2003 = pa.Table.from_pandas(self.SalesData2003)
            tableSalesData2004 = pa.Table.from_pandas(self.SalesData2004)
            tableSalesData2005 = pa.Table.from_pandas(self.SalesData2005)

            # Write Arrow Table to Parquet file
            pq.write_table(tableSalesData2003, 'parquet/SalesData_2003.parque')
            pq.write_table(tableSalesData2004, 'parquet/SalesData_2004.parque')
            pq.write_table(tableSalesData2005, 'parquet/SalesData_2005.parque')
            msg = "Sales Data saved to parquet."
            logging.info(msg)
            print(msg)
        except Exception as exSaveSalesDataToParquet:
            msg = exSaveSalesDataToParquet
            logging.info(msg)
            print(msg)
        finally:
            msg = "saveSalesDataToParquet() completed."
            logging.info(msg)
            print(msg)
    
    def getTotalSalesOfCancelledOrders(self,year):
        salesOfCancelledOrders = []
        try:
            if year==2003 or year == 0:
                print("2003")
                # salesOfCancelledOrders = df2003.loc[(df2003['status'].trim().lower() == 'cancelled'), 'sales'].sum()
                salesOfCancelledOrders2003 = salesOfCancelledOrders
            if year==2004 or year == 0:
                print("2004")
                # salesOfCancelledOrders = df2004.loc[(df2004['status'].trim().lower() == 'cancelled'), 'sales'].sum()
                salesOfCancelledOrders2004 = salesOfCancelledOrders
            if year==2005 or year == 0:
                print("2005")
                # salesOfCancelledOrders = self.SalesData2005.loc[(self.SalesData2005['status'].trim().lower() == 'cancelled'), 'sales'].sum()
                salesOfCancelledOrders2005 = salesOfCancelledOrders
            if year == 0:
                print("All")
                # salesOfCancelledOrders = pd.concat([salesOfCancelledOrders2003,salesOfCancelledOrders2004,salesOfCancelledOrders2005])
            self.resultSet['SalesOfCancelledOrders'] = salesOfCancelledOrders
            self.downloadExcel(self.resultSet['SalesOfCancelledOrders'],'salesOfCancelledOrders.xlsx')
        except Exception as exGetTotalSalesOfCancelledOrders:
            msg = "exGetTotalSalesOfCancelledOrders", exGetTotalSalesOfCancelledOrders
            logging.info(msg)
            print(msg)
        finally:
            msg = "getTotalSalesOfCancelledOrders completed."
            logging.info(msg)
            print(msg)
        
    def getTotalSalesOfOnHoldOrders(self,year):
        print("getTotalSalesOfOnHoldOrders started")
        salesOfOnHoldOrders = []
        try:
            if year==2003 or year == 0:
                # salesOfOnHoldOrders = df2003.loc[(df2003['status'].trim().lower() == 'on hold'), 'sales'].sum()
                salesOfOnHoldOrders2003 = salesOfOnHoldOrders
            if year==2004 or year == 0:
                # salesOfOnHoldOrders = df2004.loc[(df2004['status'].trim().lower() == 'on hold'), 'sales'].sum()
                salesOfOnHoldOrders2004 = salesOfOnHoldOrders
            if year==2005 or year == 0:
                # salesOfOnHoldOrders = self.SalesData2005.loc[(self.SalesData2005['status'].trim().lower() == 'on hold'), 'sales'].sum()
                salesOfOnHoldOrders2005 = salesOfOnHoldOrders
            if year == 0:
                print("All")
                # salesOfOnHoldOrders = salesOfOnHoldOrders2003 + salesOfOnHoldOrders2004 + salesOfOnHoldOrders2005
            self.downloadExcel(salesOfOnHoldOrders,"salesOfOnHoldOrders.xlsx")
        except Exception as exGetTotalSalesOfOnHoldOrders:
            msg = exGetTotalSalesOfOnHoldOrders
            logging.info(msg)
            print(msg)
        finally:
            msg = "getTotalSalesOfOnHoldOrders() completed."
            logging.info(msg)
            print(msg)
        
    def getCountOfDistinctProductsPerLine(self):
        print("getDistinctProductsPerLine")
        countOfDistinctProductsPerLine = []
        try:
            countOfDistinctProductsPerLine = self.SalesDataConsolidated.groupby('PRODUCTLINE')['PRODUCTCODE'].nunique()
            # -> count(distinct PRODUCTCODE) as productcount, product line group by product line.
            # df = df.groupby(by='domain', as_index=False).agg({'ID': pd.Series.nunique})
            # print(df)
            self.downloadExcel(countOfDistinctProductsPerLine,"countOfDistinctProductsPerLine.xlsx")
        except Exception as exCountOfDistinctProductsPerLine:
            msg = exCountOfDistinctProductsPerLine
            logging.info(msg)
            print(msg)
        finally:
            msg = "getCountOfDistinctProductsPerLine() completed."
            logging.info(msg)
            print(msg)

    def calculateVariance(self, columnName1, columnName2):
        variance = 0
        try:
            # Variance logic
            # calculate the mean
            # mean = (sum(SALES)/len(self.TotalRows))
            #calculate differences
            # diff = [(v - mean) for v in SALES]
            #Square differences and sum
            # sqr_diff = [d**2 for d in diff]
            # sum_sqr_diff = sum(sqr_diff)
            #calculate variance
            # variance = sum_sqr_diff/(len(self.TotalRows))

            # Alternatively,
            
            variance = self.SalesDataConsolidated[['"'+ columnName1 + '"']].var()
            # self.SalesDataConsolidated[['MSRP']].var()
            # self.SalesDataConsolidated.var() #print(df.var())
            # to do save to excel
            self.downloadExcel(self, variance, "data/variance.xlsx")

        except Exception as exCalculateVariance:
            msg = exCalculateVariance
            logging.info(msg)
            print(msg)
        finally:
            msg = "Finally of variance"
            logging.info(msg)
            print(msg)
        
    def calculateSalesChangeYOY(self):
        salesChangeYOY = 0
        filteredSalesData = []
        try:
            # Filter:- classic cars, years 2004 and 2005, status is shipped
            filterCar = self.SalesDataConsolidated['PRODUCTLINE'].strip().lower() == "classic cars"
            filterYear1 = self.SalesDataConsolidated['ORDERDATE'] == 2004
            filterYear2 = self.SalesDataConsolidated['ORDERDATE'] == 2005
            filterStatus = self.SalesDataConsolidated['STATUS'].strip().lower() == "shipped"

            # filtering data on basis all filters
            filteredSalesData = self.SalesDataConsolidated.loc[(filterCar)
                                                            & (filterYear1 | filterYear2)
                                                            & (filterStatus)]
            # Alternatively,
            # filteredSalesData = self.SalesDataConsolidated.where(filterCar & (filterYear1 | filterYear2) & filterStatus, inplace = True)

            self.SalesDataConsolidated['SALESCHANGEYOY'] = filteredSalesData['SALES'].pct_change(12)

            self.downloadExcel(self.SalesDataConsolidated,"calculateSalesChangeYOY.xlsx")

        except Exception as exCalculateSalesChangeYOY:
            msg = exCalculateSalesChangeYOY
            logging.info(msg)
            print(msg)
        finally:
            msg = "Finally of calculateSalesChangeYOY"
            logging.info(msg)
            print(msg)
    
    
    def filterDatasetByProductLines(self):
        filterDatasetByProductLines = []
        try:
            # ‘Vintage Cars’, ‘Classic Cars’, ‘Motorcycles’, ‘Trucks and Buses’
            filterCar1 = self.SalesDataConsolidated['PRODUCTLINE'].trim().lower() == "vintage car"
            filterCar2 = self.SalesDataConsolidated['PRODUCTLINE'].trim().lower() == "classic cars"
            filterCar3 = self.SalesDataConsolidated['PRODUCTLINE'].trim().lower() == "motorcycles"
            filterCar4 = self.SalesDataConsolidated['PRODUCTLINE'].trim().lower() == "trucks and buses"

            filterDatasetByProductLines = self.SalesDataConsolidated.loc[(filterCar1) | (filterCar2) | (filterCar3) | (filterCar4)]
            self.downloadExcel(filterDatasetByProductLines, "filterDatasetByProductLines.xlsx")

        except Exception as exfilterDatasetByProductLines:
            msg = exfilterDatasetByProductLines
            logging.info(msg)
            print(msg)
        finally:
            msg = "Finally of filterDatasetByProductLines"
            logging.info(msg)
            print(msg)
    
    def calculateSalesUsingMSRP(self):
        try:
           self.SalesDataConsolidated['SalesUsingMSRP'] =  self.SalesDataConsolidated['MSRP'] * self.SalesDataConsolidated['QUANTITYORDERED']
        except Exception as excalculateSalesUsingMSRP:
            print(excalculateSalesUsingMSRP)
        finally:
            print('calculateSalesUsingMSRP exit')


objSalesDataProjection = SalesDataProjection()

#region 1. Save Sales Data To Parquet
# A transformed extract to be saved in a parquet format partitioned by the existing 'OrderDate' 
# column into daily partitions. E.g. {base_dir}/Year=yyyy/Month=mm/Day=dd/{filename}.parque
objSalesDataProjection.saveSalesDataToParquet()
#endregion Save Sales Data To Parquet

#region 2.1 Total Sales Of Cancelled Orders
# What is the total sales value of the cancelled orders? 
objSalesDataProjection.getTotalSalesOfCancelledOrders(0)
#endregion Total Sales Of Cancelled Orders

#region 2.2 Total Sales Of OnHold Orders
# What is the total sales value of the orders currently on hold for the year 2005?
salesOfOnHoldOrders = objSalesDataProjection.getTotalSalesOfOnHoldOrders(2005)
#endregion Total Sales Of OnHold Orders

#region 2.3 Count of distinct products per product line
# What is the count of distinct products per product line?
countOfDistinctProductsPerLine = objSalesDataProjection.getCountOfDistinctProductsPerLine()
#endregion Count of distinct products per product line

#region 2.4 Total sales variance for sales price and MSRP
# What is the total sales variance for sales calculated at both sales price and MSRP (Manufacturer Suggested Retail Price)
objSalesDataProjection.calculateVariance('SALES','MSRPSales')
#endregion Total sales variance for sales price and MSRP

#region 2.5 Percent change in sales YOY and filters
# What has been the percentage change in sales YoY for classic cars, for years 2004 and 2005, where the status is shipped?
objSalesDataProjection.calculateSalesChangeYOY()
#endregion Percent change in sales YOY and filters

#region 3.1 Filter dataset by Product Lines
# Dataset should be filtered for the following product lines; ‘Vintage Cars’, ‘Classic Cars’, ‘Motorcycles’, ‘Trucks and Buses’
objSalesDataProjection.filterDatasetByProductLines()
#endregion

#region 3.2 Discounted Price
# TBD using switch case for range of "sales"
#endregion Discounted Price

#region 3.3 Add calculated column by recalculating sales using MSRP
objSalesDataProjection.calculateSalesUsingMSRP()
#endregion