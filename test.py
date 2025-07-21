import polars as pl

data = {
    "id": [1, 2, 3, 4, 5, 6, 7, 8],
    "success": [1, 0, 1, 0, 0, 1, 0, 1],
    "name": ["bob", "tom", "mick", None, "ben", "eva", "udo", "pam"],
    "boost": [1, 0, 1, 1, 0, 1, 0, None]
}

df = pl.DataFrame(data)
df.filter(pl.col('success') == 1)

def describe(df: pl.DataFrame) -> pl.DataFrame:

    description_pl = df.describe()
    df_rows = df.shape[0]
    df_cols = df.shape[1]

    out_variable = description_pl.drop(pl.col('statistic')).columns
    out_type = description_pl.drop(pl.col('statistic')).dtypes
    out_type = [str(x) for x in out_type]
    out_obs = [df_rows] * df_cols
    out_na = description_pl.filter(pl.col('statistic') == 'null_count').drop(pl.col('statistic')) 
    out_na = out_na.cast(pl.Int64)
    out_na = out_na[0, ].transpose().to_series().cast(pl.Int64).to_list()
    out_na_pct = [x * 100.00 / df_rows for x in out_na]
    out_min = description_pl.filter(pl.col('statistic') == 'min').drop(pl.col('statistic'))
    out_min = out_min.cast(pl.Int64, strict = False)
    out_min = out_min[0, ].transpose().to_series().to_list()
    out_mean = description_pl.filter(pl.col('statistic') == 'mean').drop(pl.col('statistic'))
    out_mean = out_mean.cast(pl.Int64, strict = False)
    out_mean = out_mean[0, ].transpose().to_series().to_list()
    out_max = description_pl.filter(pl.col('statistic') == 'max').drop(pl.col('statistic'))
    out_max = out_max.cast(pl.Int64, strict = False)
    out_max = out_max[0, ].transpose().to_series().to_list()

    out = pl.DataFrame({
        "variable": out_variable,
        "type": out_type,
        "obs": out_obs,
        "na": out_na,
        "na_pct": out_na_pct,
        "min": out_min,
        "mean": out_mean,
        "max": out_max
    })

    return out


out = describe(df)
out
out.filter(pl.col('type') == "Float64")

def use_data_beer() -> pl.DataFrame:
    data = pl.read_csv("data/beer.csv", has_header=True, separator=",")
    return data

df = use_data_beer()
describe(df)

def use_data_penguins() -> pl.DataFrame:
    data = pl.read_csv("data/penguins.csv", has_header=True, separator=",")
    return data

df = use_data_penguins()
describe(df)

def use_data_iris() -> pl.DataFrame:
    data = pl.read_csv("data/iris.csv", has_header=True, separator=",")
    data = data.rename({
        "Sepal.Length": "sepal_legth",
        "Sepal.Width": "sepal_width",
        "Petal.Length": "petal_length",
        "Petal.Width": "petal_width",
        "Species": "species"})
    return data

df = use_data_iris()
describe(df)

def use_data_buy() -> pl.DataFrame:
    data = pl.read_csv("data/buy.csv", has_header=True, separator=",")
    return data

df = use_data_buy()
describe(df)

def use_data_titanic() -> pl.DataFrame:
    data = pl.read_csv("data/titanic.csv", has_header=True, separator=",")
    data = data.rename({
        "Class": "class",
        "Sex": "gender",
        "Age": "age",
        "Survived": "survived"})
    return data

df = use_data_titanic()
describe(df)

def use_data_wordle() -> pl.DataFrame:
    data = pl.read_csv("data/wordle.csv", has_header=True, separator=",")
    return data

df = use_data_wordle()
describe(df)

def use_data_esoteric() -> pl.DataFrame:
    data = pl.read_csv("data/esoteric.csv", has_header=True, separator=",")
    return data

df = use_data_esoteric()
describe(df)
