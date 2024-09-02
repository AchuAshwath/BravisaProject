# Market Quality Number Process
I have written Python codes for an Market Quality Number Process for NSE500, NFITY  and BTT Index.

    1. NSE500 MQN : Generate MQN values for NSE500
    2. NIFTY MQN : Generate MQN values for NIFTY
    3. BTT Index MQN : Generate MQN values for BTT Index

A Market Quality Number (MQN) is a Process which Generate a Daily MQN values for NSE500, NIFTY and BTT Index.This process is calculated to plot a graph based upon MQN value and the date on the Bravisa Dash Board.There will be three different graphs such as NSE500 MQN graph,
NIFTY MQN graph, BTT Index MQN graph.The MQN process is calculated based upon various parameters and formula.This are the following steps which is used to calculated MQN process.

#### Step 1 : Fetch the Latest 42 instance data  of NSE500, NIFTY and BTT Index. 

#### Step 2 : Calculated the ATR values with 21 days of time period using formula. 

            Current ATR = [(Prior ATR x 20) + Current TR] / 21.

#### Step 3 : Calculate ATR Average value using formula. 

            ATR Average = ( Current ATR / Current Close ) * 100

#### Step 4 : Calculate Standard Deviation(SD) and Mean(μ) for 42 days of ATR Average and generate three values based upon formula.

            Very Volatile value =  μ + (SD * 3)

            Volatile value =  μ + (SD * 0.5)

            Normal value =  μ - (SD * 0.5)

#### Step 5 : From this conditions, Market is found in four parameters, such as Very Volatile , Volatile, Normal, and Quite.

            condition 1 :  "Very Volatile" =  ATR Average >= Very Volatile value

            condition 2 :  "Volatile" =  ATR Average < Very Volatile value
                                         and ATR Average >= Volatile value

            condition 3 :  "Normal" =  ATR Average < Volatile value and 
                                    ATR Average >= Normal Value 

            condition 4 :  "Quite" =  ATR Average < Normal Value 

#### Step 6 : To calculate MQN value we require a 100 days Change Average(μ) value and 100 days Standard Deviation(SD) value of NSE500, NIFTY and BTT Index. Using this values and formula we can produce MQN value. Using following formula.

            MQN value = ( 100 day change Average / 100 days Standard Deviation) * 10

#### Step 7 : Based upon MQN value and some parameterized values we will generate another MQN condition of NSE500, NIFTY and BTT Index. MQN conditions formula as follows,

            condition 1 : "Strong Bullish" = MQN value >= 1.47

            condition 2 : "Bullish" = MQN value < 1.47 and MQN value >= 0.7

            condition 3 : "Neutral" = MQN value < 0.7 and MQN value >= 0

            condition 4 : "Bearish" = MQN value < 0 and MQN value >= -0.7

            condition 5 : "Strong Bearish" = MQN value < -0.7



        
