import polars as pl

data = {
    "id": [1, 2, 3, 4, 5, 6, 7, 8],
    "success": [1, 0, 1, 0, 0, 1, 0, 1],
    "boost": [1, 0, 1, 1, 0, 1, 0, None]
}

df = pl.DataFrame(data)
df.filter(pl.col('success') == 1)

description_pl = df.describe()
description_pl

out_variable = description_pl.drop(pl.col('statistic')).columns
out_type = description_pl.drop(pl.col('statistic')).dtypes
out_type = [str(x) for x in out_type]
out_na = description_pl.filter(pl.col('statistic') == 'null_count').drop(pl.col('statistic')) 
out_na = out_na[0, ].transpose().to_series().cast(pl.Int64).to_list()
out_min = description_pl.filter(pl.col('statistic') == 'min').drop(pl.col('statistic'))
out_min = out_min[0, ].transpose().to_series().to_list()
out_mean = description_pl.filter(pl.col('statistic') == 'mean').drop(pl.col('statistic'))
out_mean = out_mean[0, ].transpose().to_series().to_list()
out_max = description_pl.filter(pl.col('statistic') == 'max').drop(pl.col('statistic'))
out_max = out_max[0, ].transpose().to_series().to_list()

out = pl.DataFrame({
    "variable": out_variable,
    "type": out_type,
    "na": out_na,
    "min": out_min,
    "mean": out_mean,
    "max": out_max
})

out
out.filter(pl.col('type') == pl.Float64)

