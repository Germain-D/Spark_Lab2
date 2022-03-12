package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import javax.xml.crypto.Data

object DataFrame {
      
  def loadData(): DataFrame = {
    val pathToFile = "/home/germain/Downloads/spark-df/src/main/resources/codesPostaux.csv"

    val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;

    spark.read
         .format("csv")
         .option("header", "true") 
         .option("mode", "DROPMALFORMED")
         .option("delimiter", ";")
         .load(pathToFile) 

    
    
  }

  //Affichez le nombre de communes.
  def countcom(): Long = {
    loadData().select("Nom_commune").count()
  }

  //Affichez le nombre de communes qui possèdent l’attribut Ligne_5
  def countcomligne5(): Long = {
    val df = loadData()
    df.select("Nom_commune").filter("Ligne_5 is not null").count()
  }

  //Ajoutez aux données une colonne contenant le numéro de département de la commune. 
  def ajoutcol(): DataFrame = {
     loadData().withColumn("departement", (col("Code_postal").substr(0,2)))
  }

  //Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”, ayant pour colonne Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
  def creenouvcsv() : Unit = {
    ajoutcol().write.option("header",true).option("delimiter",";").csv("/home/germain/Downloads/spark-df/src/main/resources/commune_et_departement.csv")
  }

  //affichez les communes du département de l’Aisne.
  def trouveraisne() : DataFrame = {
    ajoutcol().select("Nom_commune").filter((col("departement") === "02"))
  }

  //Quel est le département avec le plus de communes ?
  def deppluscommunes(): DataFrame = {
    ajoutcol().select("departement", "Code_commune_INSEE")
                    .distinct()
                    .groupBy("departement")
                    .count()
                    .orderBy(desc("Count"))
  }

  def main(args: Array[String]): Unit = {

    val df = loadData()
    df.show(5)

    // Quel est le schéma du fichier ? 
    df.printSchema()

    //Affichez le nombre de communes.
    println("Nombre de communes : " + countcom())

    //Affichez le nombre de communes qui possèdent l’attribut Ligne_5
    println("Nombre de communes avec ligne 5 : " + countcomligne5())

    //Ajoutez aux données une colonne contenant le numéro de département de la commune. 
    ajoutcol().show(5)

    //Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”, ayant pour colonne Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
    //creenouvcsv()

    //affichez les communes du département de l’Aisne.
    trouveraisne().show(5)

    //Quel est le département avec le plus de communes ?
    deppluscommunes().show(1)

   }


}
