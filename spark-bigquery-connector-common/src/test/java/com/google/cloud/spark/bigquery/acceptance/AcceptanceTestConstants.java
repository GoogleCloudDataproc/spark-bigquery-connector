package com.google.spark.bigquery.acceptance;

public class AcceptanceTestConstants {

  public static final String MIN_BIG_NUMERIC =
      "-578960446186580977117854925043439539266.34992332820282019728792003956564819968";

  public static final String MAX_BIG_NUMERIC =
      "578960446186580977117854925043439539266.34992332820282019728792003956564819967";

  public static final String BIGNUMERIC_TABLE_QUERY_TEMPLATE =
      "create table %s.%s (\n"
      + "    min bignumeric,\n"
      + "    max bignumeric\n"
      + "    ) \n"
      + "    as \n"
      + "    select \n"
      + "    cast(\"" + MIN_BIG_NUMERIC + "\" as bignumeric) as min,\n"
      + "    cast(\"" + MAX_BIG_NUMERIC + "\" as bignumeric) as max";


}
