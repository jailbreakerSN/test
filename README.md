# Algorithme d analyse de mouvement de population

Fort de plus de trente millions de clients, le **Groupe SONATEL** stocke journalièrement une très grande masse de données mobiles (appels, SMS, sonde…).Celles-ci s’avèrent être une importante mine d’informations, à condition qu’elles soient mise à la disposition de chercheurs et data scientist, qui seront à mesure d’y appliquer des algorithmes afin d’en extraire une multitude d’information utiles à la prise de décisions pour des clients comme les structures administratives, les entreprises, les organisations non gouvernementales etc... 
Pour ce Usecase, nous avons procédé  au développement d’un **algorithme d’analyse de mouvement de populations** avec les outils et technologies **Big Data**. Il sera question, à partir des relevés détaillés de communications ou **CDR** (Call Detail Records), de modéliser un algorithme capable de déterminer les lieux d’habitations (**home location**) et de travail (**work location**) des clients. Ainsi, l’application de cet algorithme sur des périodes antérieures et postérieures à la mise en place d’infrastructures comme l’autoroute à péage ou le Train Express Régionale permettra d’avoir une idée sur l’impact de ces travaux sur le désengorgement de la ville de Dakar, avec à l’appui des statistiques réelles sur les changements de lieux d’habitation et de travail des populations.

L'implémentation de cette algorithme peut-être explicitée en quatres grandes étapes, allant de l'acquisition des données d'antennes et CDR jusqu'à l'ingestion dans la base de données Hive, en passant par l'extraction dans la plateforme de fichiers distribués HDFS, jusqu'à la détermination des home et work location des utilisateurs présents dans notre base de données CDR.

## Extraction et Ingestion des Données
 Il s'agit de la première étape dans l'implémentation de notre algorithme. Elle consiste en l'acquisition des fichiers compréssés des relevés détaillés de communications (CDR) issus des serveurs dédiés. Une fois les données acquises au niveau de notre Namenode, on procedera alors au dézippage des fichiers puis à leur insertion au niveau des Datanodes. Cette dernière étape nous permettra d'avoir l'ensemble des données dans notre système de fichiers distribués de Hadoop (HDFS) et donc de bénéficier de tous les avantages connexes. 
 ```
Give examples
```
Il nous sera donc facile de créer la table de Hive permettant de pointer directement au niveau du repertoire de dezippage et d'appliquer le parsing adéquat pour formatter les données, de manière à pouvoir recupérer l'ensemble des données sous format d'une table structurée.
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
 C'est une étape très importante car permettant d'effectuer un ensemble d'opérations et de jointures afin d'agréger les données sous un format consommable par notre module de clustérisation. En effet, les données CDR formattées ne peuvent avoir de réelles signification à leur état brute. De par leur volume assez important, nous avons d'abord procédé par une extraction des informations relatives à une population de 100000 clients de part et d'autre des jeux de données des mois de juillet et novembre.
 ```
Give examples
```
