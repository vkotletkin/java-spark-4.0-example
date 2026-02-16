# Geo

1) `CREATE EXTENSION IF NOT EXISTS postgis_raster`
2) `raster2pgsql -s 4326 -t 100x100 -N -32768 -c -I -C -M -F /data/*.hgt public.srtm > /tmp/all_srtm.sql`
3) `psql -U postgres -d postgres -q -f /tmp/all_srtm.sql`

4) -javaagent:enc-sniarbtej-2025.12.10.jar=id=sniarbtej,user=JetBrains,exp=2048-10-24,force=true


Check metadata:

```sql
SELECT rid,
       filename,
       ST_SRID(rast)       as srid,
       ST_UpperLeftX(rast) as ul_x, -- долгота левого верхнего угла
       ST_UpperLeftY(rast) as ul_y, -- широта левого верхнего угла
       ST_ScaleX(rast)     as pixel_size_x,
       ST_Width(rast)      as width_px
FROM public.srtm
```

Check polygon:

```sql
SELECT ST_AsText(ST_ConvexHull(ST_Union(rast::geometry)))
FROM public.srtm;

SELECT ST_XMin(ST_Extent(rast::geometry)) as min_lon,
       ST_XMax(ST_Extent(rast::geometry)) as max_lon,
       ST_YMin(ST_Extent(rast::geometry)) as min_lat,
       ST_YMax(ST_Extent(rast::geometry)) as max_lat
FROM public.srtm;
```

Check point:

```sql
SELECT ST_Value(rast, ST_SetSRID(ST_Point(30.515671, 56.4), 4326)) AS elevation
FROM public.srtm
WHERE ST_Intersects(rast, ST_SetSRID(ST_Point(30.515671, 56.4), 4326));
```

