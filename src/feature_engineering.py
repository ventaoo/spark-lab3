from pyspark.ml.feature import VectorAssembler, StandardScaler

def assemble_and_scale(df, input_cols):
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    df_vec = assembler.transform(df)
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_vec)
    return scaler_model.transform(df_vec)
