# Algorithme d analyse de mouvement de population

Fort de plus de trente millions de clients, le **Groupe SONATEL** stocke journalièrement une très grande masse de données mobiles (appels, SMS, sonde…).Celles-ci s’avèrent être une importante mine d’informations, à condition qu’elles soient mise à la disposition de chercheurs et data scientist, qui seront à mesure d’y appliquer des algorithmes afin d’en extraire une multitude d’information utiles à la prise de décisions pour des clients comme les structures administratives, les entreprises, les organisations non gouvernementales etc... 
Pour ce Usecase, nous avons procédé  au développement d’un **algorithme d’analyse de mouvement de populations** avec les outils et technologies **Big Data**. Il sera question, à partir des relevés détaillés de communications ou **CDR** (Call Detail Records), de modéliser un algorithme capable de déterminer les lieux d’habitations (**home location**) et de travail (**work location**) des clients. Ainsi, l’application de cet algorithme sur des périodes antérieures et postérieures à la mise en place d’infrastructures comme l’autoroute à péage ou le Train Express Régionale permettra d’avoir une idée sur l’impact de ces travaux sur le désengorgement de la ville de Dakar, avec à l’appui des statistiques réelles sur les changements de lieux d’habitation et de travail des populations.

L'implémentation de cette algorithme peut-être explicitée en quatres grandes étapes, allant de l'acquisition des données d'antennes et CDR jusqu'à l'ingestion dans la base de données Hive, en passant par l'extraction dans la plateforme de fichiers distribués HDFS, jusqu'à la détermination des home et work location des utilisateurs présents dans notre base de données CDR.

## Extraction et Ingestion des Données
 Il s'agit de la première étape dans l'implémentation de notre algorithme. Elle consiste en l'acquisition des fichiers compréssés des relevés détaillés de communications (CDR) issus des serveurs dédiés. Une fois les données acquises au niveau de notre Namenode, on procedera alors au dézippage des fichiers puis à leur insertion au niveau des Datanodes. Ceci nous permettra d'avoir l'ensemble des données dans notre système de fichiers distribués de Hadoop (HDFS) et donc de bénéficier de tous les avantages connexes. 
 ```
#!/bin/bash
set -e
for file in $1
do 
 fileToErase=`echo $file | cut -d'.' -f 1,2`
 echo $fileToErase
 gunzip $file 
 hadoop fs  -put $fileToErase  /data2/staging/cdrnm/sample_jul || true 
 gzip $fileToErase 
done
```
Il nous sera donc facile, après l'execution de ce script shell, de créer la table de Hive permettant de pointer directement au niveau du repertoire de dezippage et d'appliquer le parsing adéquat pour formatter les données, de manière à pouvoir recupérer l'ensemble des données sous format d'une table structurée.
 ```
CREATE EXTERNAL TABLE cdr_datas(
        Sequence_Number String,
        Version String,
        Switch_Type String,
        Call_Record_Type String,
        Subscription_Type String,
        Call_Termination_Type String,
        Call_Termination_Error_Code String,
        Mobile_Subscriber_Identity String,
        Caller_MSISDN_Type String,
        Caller_RN String,
        Caller_MSISDN String,
        Call_Partner_Identity_Type String,
        Call_Partner_Identity String,
        Basic_Service String,
        Bearer_Capability String,
        Call_Date String,
        Call_Time String,
        Call_Duration String,
        MSC_Identity_Type String,
        MSC_Identity String,
        MS_Location_Type String,
        MS_Location String,
        MS_Location_Extension_Type String,
        MS_Location_Extension String,
        Equipment_Identity String,
        Status_of_Equipment String,
        Call_Origin String,
        Channel_Type String,
        Link_Identity String,
        PSTN_Charge String,
        Supplementary_Services String,
        Outgoing_Trunk_Group String,
        Incoming_Trunk_Group String,
        Filler String
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
        "input.regex" = "(.{10})(.{3})(.{1})(.{1})(.{1})(.{1})(.{1})(.{15})(.{1})(.{4})(.{11})(.{1})(.{15})(.{2})(.{2})(.{8})(.{6})(.{6})(.{1})(.{15})(.{1})(.{18})(.{1})(.{15})(.{15})(.{1})(.{1})(.{1})(.{5})(.{8})(.{8})(.{10})(.{10})(.{37}).*",
        "output.format.string" = "%11$s %16$s %17$s %22$s"
)
LOCATION '/data2/staging/cdrnm/sample_nov/cdr_datas';
```
## Pré-processing
 C'est une étape très importante car permettant d'effectuer un ensemble d'opérations et de jointures afin d'agréger les données sous un format consommable par notre module de clustérisation. En effet, les données CDR formattées ne peuvent avoir de réelles significations à leur état brute. 
 De par leur volume assez important, nous avons d'abord procédé par une extraction des informations relatives à une population de 100000 clients de Dakar, présents de part et d'autre des jeux de données des mois de juillet et novembre.
 ```
CREATE TABLE algorithmisation.subset_of_cdr_larger as 
select distinct M.caller_msisdn 
from (
    select S.caller_msisdn, T.region 
    from algorithmisation.cdr_datas S left join algorithmisation.antennas T on (S.ms_location=T.id_cellule) 
    where region="Dakar"
    ) M 
where rand()<=0.1 distribute by rand() sort by rand() limit 100000";
```
Pour cibler toutes les données relatives à ces clients au niveau de la table CDR du mois de juillet, nous executons le script suivant
 ```
 CREATE TABLE algorithmisation.subset_of_cdr_July as 
select * from algo_july.cdr_datas 
where caller_msisdn in table algorithmisation.subset_of_cdr_larger ";
```
Nous effectuerons la même opération sur la table Hive realtive aux données CDR du mois de novembre. 
Une fois ces tables de subset créées, nous passons à les premières opérations de traitement sur celles-ci et qui consistent à recupérer le nombre de jour d'appel qu'a effectué chaque utilisateur du réseau durant le mois concerné sur chacune des antennes et de procéder ainsi à un ranking par rapport au nombre de jour d'appel enregistré sur les différentes antennes. Pour éviter tout résultat biaisé, nous considérerons que plusieurs appels effectués le même jour sur une même antenne constitueront un même jour d'appel.
 ```
create table algorithmisation.person_tower_days as 
select M.caller_msisdn, M.bts_nodeb, M.longitude, M.latitude, count(DISTINCT M.call_date) as tot_days 
from (
    select S.caller_msisdn, S.call_date, S.call_time, T.bts_nodeb, T.longitude, T.latitude 
    from algorithmisation.subset_cdr_data S inner join algorithmisation.antennas T on (S.ms_location=T.id_cellule)
    ) M 
group by M.caller_msisdn, M.bts_nodeb, M.latitude, M.longitude;
```
Voici le cript Hive qui permettra de calculer et d'enregistrer le nombre total d'antennes utilisées par utilisateurs durant le mois de CDR donné.
 ```
create table algorithmisation.person_towers as 
select caller_msisdn, count(DISTINCT bts_nodeb) as total_towers 
from algorithmisation.person_tower_days group by caller_msisdn;
```
la jointure de ces 2 précédentes tables permet alors de créer une nouvelle table avec le nombre de jours d'appel par utilisateur et par antenne, mais aussi d'avoir le classement par utilisateur, de l'antenne la plus sollicitée à celle la moins utilisée durant le mois.
 ```
create table algorithmisation.person_tower_days_final as 
select R.*, rank() over (partition by R.caller_msisdn order by R.tot_days desc, rand() desc) as rnk 
from (
    select S.caller_msisdn, S.bts_nodeb, S.longitude, S.latitude, S.tot_days, M.total_towers 
    from algorithmisation.person_tower_days S inner join algorithmisation.person_towers  M  on (S.caller_msisdn=M.caller_msisdn)
    ) R;
```
On n'obtient alors à l'issue de cette étape de prétraitement, en plus de la table initiale (subset_of_cdr_july et subset_of_cdr_november) contenant l'ensemble des informations (heure, jour, antenne etc...)des opérations effectuées, le nombre d'antennes et de jours d'appels, avec un classement par ordre de sollicitation de l'antenne.

## Clustérisation par centroide pondéré
C'est une étape qui s'inscrit dans la continuité, en utilisant les résultats des scripts exécutés précédemment. Cependant elle diffère de celles-ci dans la mesure ou elle est codée en Scala, parce que nécessitant des fonctions et méthodes propriétaires mais aussi et surtout utilise la technologie Spark pour gagner en rapidité, dans la mesure ou nous allons procéder à des calculs hautement complexes sur l'ensemble des données (regrouper les informations par utilisateurs, jointures avec la table des antennes, calcul récursif de centroides pondérés, regroupement par utilisateur par cluster).
Nous avons procédé par la création d'un projet Spark-Scala modulable, avec un fichier de configuration externe contenant les informations des bases de données et tables Hive utilisées dans le code. L'essentiel est d'avoir en entrée les données escomptées et donc indépendamment du jeu de données utilisées (CDR), l'execution du code produit les résultats escomptés (clustérisation par utilisateurs).
* **Quelques extraits et explications du code Spark-Scala**
 ```
/**
  * Classe permettant de stocker pour chaque utilisateur la liste des antennes qu'il a eu à utiliser
  * @param msisdn msisdn du client
  * @param listLocation une liste d'antennes avec les coordonnées GPS (latitude, longitude)
  */
class UserData(var msisdn: String, var listLocation: List[Antenna]) {
  /**
    * Methode qui utilise la liste des antennes de l'utilsateur courant pour clustériser
    * @return Une liste de Clusters contenant chacun des antennes distantes de moins de 2.5 kms 
    */
  def getCluster(): List[Cluster] = {
    new Create_Clusters(5).getClusters(listLocation)
  }
}
```
 ```
/**
      * Une fonction récursive (Tail recursive function) qui charge une liste d'antennes (de point d'appels) en entrée
      * pour mettre chacune d'elles dans sont cluster appropié, suivant une distance pondéré
      *
      * @param listAntennas une Liste d'antennes (Point géographique d'appels des clients)
      * @param accum        Accumulateur qui garde l'ensemble des Cluster
      * @return Une Liste de Cluster
      */
    @tailrec
    def clusterAccumulator(listAntennas: List[Antenna], accum: List[Cluster]): List[Cluster] = {
      var nextPoint = new Antenna()
      listAntennas match {
        case Nil => accum
        case x :: tail => {
          if (indic == 0) {
            cl = new Cluster()
          }
          var p = x
          if (!tail.isEmpty) {
            nextPoint = tail.head
          }
          cl.addAntenna(p)
          var newTail = p.listSorted(tail)
          //println("First Point" + p + " Next Point" + nextPoint)
          if (utils.calculDistance(p.point, nextPoint.point) > 2.5 /* p.calculateThreesold(forThreesold, rnk)*/ ) {
            indic = 0
            clusterAccumulator(newTail, cl :: accum)
          } else {
            indic = 1
            clusterAccumulator(newTail, accum)
          }
        }
      }
    }
```
```
/**
    * Calcul de la distance entre 2 points géographiques avec les coordonées Latitude et Longitude
    * @return la distance en Km entre les 2 points
    */
  def calculDistance(): Double = {

    val latDistance = A_rad.latitude - B_rad.latitude
    val lngDistance = A_rad.longitude - B_rad.longitude

    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)

    val a = sinLat * sinLat +
      (Math.cos(A_rad.latitude)
        * Math.cos(B_rad.latitude)
        * sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    AVERAGE_RADIUS_OF_EARTH_KM * c
  }
 ```
```
 /**
    * Methode appelée lors de la clutérisation d'une liste de points d'appels d'un client
    * qui calcule le centre de deux points d'appels géographiques,
    * pondérés par rapport au nombres d'appels effectués sur chaque point d'appel
    * @return Un nouveau point
    */
  def centroidePond(): Point = {

    // Conversion des coordonnées du Point A en coordonnées cartésiennes
    var x1 = Math.cos(A_rad.latitude) * Math.cos(A_rad.longitude)
    var y1 = Math.cos(A_rad.latitude) * Math.sin(A_rad.longitude)
    var z1 = Math.sin(A_rad.latitude)
    // Conversion des coordonnées du Point B en coordonnées cartésiennes
    var x2 = Math.cos(B_rad.latitude) * Math.cos(B_rad.longitude)
    var y2 = Math.cos(B_rad.latitude) * Math.sin(B_rad.longitude)
    var z2 = Math.sin(B_rad.latitude)

    // Calcul des coordonnées du centroide pondéré par le poids de chaque point
    var totPond = point_A.weight + point_B.weight
    val pond_A = (point_A.weight / totPond)
    val pond_B = (point_B.weight / totPond)
    var xm = x1 * pond_A + x2 * pond_B
    var ym = y1 * pond_A + y2 * pond_B
    var zm = z1 * pond_A + z2 * pond_B
    // Recupération des coordonnées latitude et longitude du centroide en radian
    var lon = Math.atan2(ym, xm)
    var hyp = Math.sqrt(xm * xm + ym * ym)
    var lat = Math.atan2(zm, hyp)

    toDegree(new Point(lat, lon, point_A.weight + point_B.weight))
  }
```
 ```
Give examples
```
