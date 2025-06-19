import pandas as pd

def predict_discharge(net_head):
    return (-2213.138124 + (56.016817 * net_head) +
            -(0.41773091 * (net_head**2)) + (0.0010005391 * (net_head**3)))

def predict_power_output(net_head):
    return 35694.485885 + -1190.729446 * net_head + 14.85438048 * net_head**2 + -0.08168483 * net_head**3 + 0.00016725 * net_head**4

def volume_discharged(UR_current, UR_prev,LR_current, LR_prev):
    def divide_into_90_divisions(start,end):
        return [start] * 900 if start == end else [start + i * ((end - start) / 899) for i in range(900)]
    res = divide_into_90_divisions(UR_current, UR_prev)
    res1 = divide_into_90_divisions(LR_current,LR_prev)
    net_head_lst = [a - b for a, b in zip(res, res1)]
    discharge_lst = [predict_discharge(i) for i in net_head_lst]
    return sum(discharge_lst)

df = pd.read_excel("/home/gopi/doker_airflow/hydro/combine_upper_data.xlsx")
cols = ['U1', 'U2', 'U3', 'U4', 'U5']
df['Plant_Power'] = df[cols].where(df[cols].abs() > 200).mean(axis=1)
df['Net Head'] = df['Uppereservoir'] - df['Lowerreservoir']
df.drop(columns=['U1','U2','U3','U4','U5'],inplace=True)
df['Upper_prev'] = df['Uppereservoir'].shift(1)
df['Lower_prev'] = df['Lowerreservoir'].shift(1)
df['predicted_power'] = predict_power_output(df['Net Head'])

df['volume'] = df.apply(
    lambda row: volume_discharged(
        row['Uppereservoir'],
        row['Upper_prev'],
        row['Lowerreservoir'],
        row['Lower_prev']
    ),
    axis=1
)
print(df)
df.to_csv("/home/gopi/doker_airflow/hydro/test.csv")