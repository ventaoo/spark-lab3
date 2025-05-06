FROM bitnami/spark:3.3.2

WORKDIR /opt/app

# 拷贝 SBT 构建的 jar
COPY target/scala-2.12/datamart-assembly-0.1.jar datamart.jar
# 复制配置
COPY src/main/resources/application.conf /opt/app/
