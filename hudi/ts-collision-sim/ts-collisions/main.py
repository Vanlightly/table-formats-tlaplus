import random
import pandas as pd

def generate_ts_set(period_ms, period_var_ms, duration_ms):
    generate_ts_set = set()
    lower = period_ms - period_var_ms
    upper = period_ms + period_var_ms
    ts=random.randint(0, period_ms)
    generate_ts_set.add(ts)
    while ts < duration_ms:
        ts = ts + random.randint(lower, upper)
        generate_ts_set.add(ts)

    return generate_ts_set

def run(writers, period_ms, period_var_ms, duration_ms):
    combined_ts_set = set()
    op_count = 0
    for w in range(0, writers):
        ts_set = generate_ts_set(period_ms, period_var_ms, duration_ms)
        op_count += len(ts_set)
        combined_ts_set = combined_ts_set.union(ts_set)

    return op_count, len(combined_ts_set)

def calculate_stats(df):
    grouped = df.groupby(['writers','period_ms','period_var_ms','duration_ms'])
    agg = grouped.agg(avg_collisions=('collision_count', 'mean'),
                      min_collisions=('collision_count', 'min'),
                      max_collisions=('collision_count', 'max'))
    print("Average number of collisions")
    print(agg.to_string())

    prob = grouped['collision_count'].apply(lambda x: (x[x > 0].count() / len(x)) * 100.0)
    print("")
    print("Probability of collision")
    print(prob.to_string())

def writer_dim(df, writers_min, writers_max, period_ms, period_var_pc, duration_ms, repeat):
    # print("run,writers,period_ms,period_var_ms,duration_ms,op_count,final_count,collisions")
    period_var_ms = period_ms * (period_var_pc / 100)

    for r in range(0, repeat):
        for w in range(writers_min, writers_max+1):
            op_count, final_count = run(w, period_ms, period_var_ms, duration_ms)
            collisions = op_count - final_count
            coll_occurred = collisions > 0
            df.loc[len(df.index)] = [r, op_count, final_count, collisions, coll_occurred, w, period_ms,
                                     period_var_ms, duration_ms]
            # print(f"{r},{w},{period_ms},{period_var_ms},{duration_ms},{op_count},{final_count},{collisions},{coll_occurred}")
    calculate_stats(df)

def period_dim(df, period_ms_min, period_ms_max, period_ms_step, writers, period_var_pc, duration_ms, repeat):
    # print("run,writers,period_ms,period_var_ms,duration_ms,op_count,final_count,collision_count,collision_occurred")

    for r in range(0, repeat):
        period_ms = period_ms_min
        while period_ms <= period_ms_max:
            period_var_ms = period_ms * (period_var_pc/100)
            op_count, final_count = run(writers, period_ms, period_var_ms, duration_ms)
            collisions = op_count - final_count
            coll_occurred = collisions > 0
            df.loc[len(df.index)] = [r, op_count, final_count, collisions, coll_occurred, writers,
                                     period_ms, period_var_ms, duration_ms]
            # print(f"{r},{writers},{period_ms},{period_var_ms},{duration_ms},{op_count},{final_count},{collisions}")
            period_ms += period_ms_step
    calculate_stats(df)

def hours_to_ms(h):
    return h * 60 * 60 * 1000

def minutes_to_ms(m):
    return m * 60 * 1000

def seconds_to_ms(s):
    return s * 1000

def new_df():
    return pd.DataFrame({'run': [], 'op_count': [], 'final_count': [], 'collision_count': [],
                           'collision_occurred': [], 'writers': [], 'period_ms': [],
                           'period_var_ms': [], 'duration_ms': []})

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("2-20 writers, 1 minute write interval, 24 hour duration")
    writer_dim(new_df(), 2, 20, minutes_to_ms(1), 5,
               hours_to_ms(24), 1000)

    print("")
    print("2-20 writers, 5 minute write interval, 24 hour duration")
    writer_dim(new_df(), 2, 20, minutes_to_ms(5), 5,
               hours_to_ms(24), 1000)

    print("")
    print("5 writers, 1-20 minute write interval, 24 hour duration")
    period_dim(new_df(), minutes_to_ms(1), minutes_to_ms(20), minutes_to_ms(1), 5,
               5, hours_to_ms(24), 1000)

    print("")
    print("5 writers, 1-20 minute write interval, 7 day duration")
    period_dim(new_df(), minutes_to_ms(1), minutes_to_ms(20), minutes_to_ms(1), 5,
               5, hours_to_ms(168), 1000)