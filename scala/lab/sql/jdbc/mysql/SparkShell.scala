package lab.sql.jdbc.mysql

/**
 * 
 1. MySQL 설치 on CentOS7....
  - # yum install -y http://dev.mysql.com/get/mysql-community-release-el7-5.noarch.rpm
  - # yum install mysql-community-server
  - # systemctl start mysqld
  - # systemctl enable mysqld
 
 
 2. Table 생성 및 Data 추가....
  - # cd /usr/bin //--불필요....
  - # mysql
  - mysql> show databases;
  - mysql> use mysql;
  - mysql> DROP TABLE IF EXISTS projects;
  - mysql> 
CREATE TABLE projects (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  name varchar(255),
  website varchar(255),
  manager varchar(255),
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
  - mysql> describe projects;
  - mysql> 
INSERT INTO projects (name, website, manager) VALUES ('Apache Spark', 'http://spark.apache.org', 'Michael');
INSERT INTO projects (name, website, manager) VALUES ('Apache Hive', 'http://hive.apache.org', 'Andy');
INSERT INTO projects VALUES (DEFAULT, 'Apache Kafka', 'http://kafka.apache.org', 'Justin');
INSERT INTO projects VALUES (DEFAULT, 'Apache Flink', 'http://flink.apache.org', 'Michael');
  - mysql> SELECT * FROM projects;


 3. MySQL JDBC Driver download....
  - https://dev.mysql.com/downloads/connector/j/
  - Platform Independent (Architecture Independent), ZIP Archive > Download > No thanks, just start my download.
  - unzip mysql-connector-java-5.1.39.zip
  - upload mysql-connector-java-5.1.39-bin.jar to CentOS7-14:/kikang/spark-2.0.2-bin-hadoop2.7
 
 
 4. Spark Shell 연결....
  - # ./bin/spark-shell --jars mysql-connector-java-5.1.39-bin.jar --master spark://CentOS7-14:7077
  - scala> val df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/mysql").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "projects").option("user", "root").option("password", "").load()
  - scala> df.show
  - scala> spark.read. + Tab키
  - scala> df.write. + Tab키
  
  
 5. Persist.... + Table 신규 데이터 입력.... + UnPersist....
  - scala> df.persist
  - scala> df.show
  - mysql> INSERT INTO projects (name, website, manager) VALUES ('Apache Storm', 'http://storm.apache.org', 'Andy');
  - mysql> SELECT * FROM projects;
  - scala> df.show
  - scala> df.unpersist
  - scala> df.show
 
 
 6. TempView 생성.... + SQL 쿼리....
  - scala> df.createOrReplaceTempView("projects")
  - scala> val sqldf = spark.sql("SELECT * FROM projects WHERE id > 3")
  - scala> sqldf.show
  - scala> spark.table("projects").show
 
 
 7. Read MySQL Table by SubQuery with Alias.... 
  - scala> val subdf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/mysql").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(SELECT * FROM projects WHERE id > 3) as subprojects").option("user", "root").option("password", "").load()
  - scala> subdf.show
 
 
 8. Write MySQL Table.... 
  - scala> df.show
  - scala> val prop: java.util.Properties = new java.util.Properties()
  - scala> prop.setProperty("user", "root")
  - scala> prop.setProperty("password", "")
  
  - //--SaveMode.Append => 기존 테이블에 Data Insert....
  - scala> subdf.select("name", "website", "manager").write.mode(org.apache.spark.sql.SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mysql", "projects", prop) 
  - mysql> SELECT * FROM projects;
  
  - //--SaveMode.Overwrite => 신규 테이블 생성("name", "website", "manager") + Data Insert....
  - scala> subdf.select("name", "website", "manager").write.mode(org.apache.spark.sql.SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysql", "projects_new", prop) 
  - mysql> SELECT * FROM projects_new;
  
  - //--SaveMode.Overwrite => 신규 테이블 생성("id", "name", "website", "manager") + Data Insert....
  - scala> subdf.select("id", "name", "website", "manager").write.mode(org.apache.spark.sql.SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysql", "projects_new", prop) 
  - mysql> SELECT * FROM projects_new;
  
  - //--SaveMode.Overwrite => 신규 테이블 생성("id", "name", "website", "manager", "new_id") + Data Insert....
  - scala> subdf.select("id", "name", "website", "manager").withColumn("new_id", col("id") + 100).write.mode(org.apache.spark.sql.SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysql", "projects_new", prop) 
  - mysql> SELECT * FROM projects_new;
  
  - //--SaveMode.Overwrite => 신규 테이블 생성("id", "name", "website", "manager", "website_suffix") with UDFs + Data Insert....
  - scala> val extractSuffixUDF = spark.udf.register("myUDF", (arg1: String) => arg1.substring(arg1.lastIndexOf(".")+1))
  - scala> subdf.select("id", "name", "website", "manager").withColumn("website_suffix", extractSuffixUDF(col("website"))).write.mode(org.apache.spark.sql.SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysql", "projects_new", prop) 
  - mysql> SELECT * FROM projects_new;
  
  - //--SQL 쿼리 기반....SaveMode.Overwrite => 신규 테이블 생성("id", "name", "website", "manager", "website_suffix_by_sql") with UDFs + Data Insert....
  - scala> spark.sql("SELECT id, name, website, manager, myUDF(website) as website_suffix_by_sql FROM projects").write.mode(org.apache.spark.sql.SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/mysql", "projects_new", prop) 
  - mysql> SELECT * FROM projects_new;
  
  
 9. JOIN....
  - //--Dataset JOIN....
  - scala> val people = spark.read.json("examples/src/main/resources/people.json")
  - scala> people.show
  - scala> people.schema
  - scala> people.printSchema
  - scala> df.show
  - scala> people.filter("age > 11").show
  - scala> people.filter("age > 11").join(df, people("name") === df("manager")).show
  - scala> people.filter("age > 11").join(df, people("name") === df("manager")).groupBy(df("website")).count.show
  - scala> people.filter("age > 11").join(df, people("name") === df("manager")).groupBy(df("website")).agg(avg(people("age")), max(df("id"))).show
  
  - //--SQL JOIN....
  - scala> people.createOrReplaceTempView("people")
  - scala> spark.sql("SELECT website, avg(age), max(id) FROM people a JOIN projects b ON a.name = b.manager WHERE a.age > 11 GROUP BY b.website").show
  
 * 
 */
object SparkShell {
  
}