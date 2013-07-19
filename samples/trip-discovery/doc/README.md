# Vehicle Trip Discovery with GIS Tools for Hadoop

An interesting task in highway management is to study potential impact of
driver carpooling, based on an analysis of automatically collected automobile
GPS position data. To identify potential enhancements to carpool
participation, we set out to study places that have the highest numbers of
trips with similar origin and destination locations. The source data for this
experiment consists of nearly 40 million vehicle position records assembled
from a single day of GPS-collected vehicle positions. The position data
consists of longitude and latitude along with date, time, and speed. We used
the Hadoop MapReduce framework for parallel computation, considering its
capability for analyzing larger data sets.

The study area was the country of Japan, selected from World Map in the data
included with ArcGIS. It was projected to the Tokyo Geographic Coordinate
System, with the _Project_ geoprocessing tool in ArcMap, to match the spatial
reference of the GPS data.  

![[GP Tool: Project to Tokyo GCS]](project-japan-to-tgcs.png)  

  

Next, the study area was exported to JSON format, suitable for use in
computations on Hadoop, by using the _Features To JSON_ tool.

![[GP Tool: Features To JSON]](features-json-japan-arcmap.png)  

The input feature class is the country of Japan in Tokyo GCS, as produced in
the previous step.  The output JSON parameter is a filename of our choosing,
with `.json` extension.  For the remaining parameters, the defaults are
suitable: JSON type (ENCLOSED_JSON) and Formatted JSON (unchecked).  

Then the JSON file representing the features, was copied to HDFS, to be
accessible to the computations on Hadoop.  This can be done either with
command-line `hadoop` `fs`, or by using the _Copy To HDFS_ tool.

![[GP Tool: Copy To HDFS]](copy-to-hdfs-japan-arcmap.png)  

The tool expects the following parameters:

  * The Input local file, is the JSON file output by the previous step.
  * The HDFS parameters for hostname, port, and username - these relate to the configuration of Hadoop; values can be provided by the system administrator (the default port number should work unless the admin has configured a non-default port).
  * The HDFS remote file - the path and file name where the tool will copy the file.  

Calculations were done in two stages of MapReduce applications on Hadoop. As a
cursory overview of MapReduce: first a Mapper associates a list of keys each
with a list of values, then for each key, a Reducer performs a calculation on
the values for that key, and emits any output records for that key. The first
MapReduce application used in our study, creates a grid of cells covering
Japan, infers origin and destination of trips from sequences of vehicle
positions, and identifies the grid cell containing the origin and destination
point of each inferred trip.  

Columns of the CSV file of GPS positions, include vehicle ID, date, time,
position as longitude and latitude (degrees-minutes-seconds), and speed
(km/h). The mapper of the first-stage MapReduce application, reads the input
CSV data, and treats the combination of car-ID and date as the key - and the
remainder of the input fields, as the value associated with such key.  Thus,
the data is grouped by car and date, for passing to the reducer, a separate
list of position records for each car and date.  

    
    
    public class TripCellMapper extends Mapper<LongWritable, Text, Text, CarSortWritable> {
    
        // column indices for values in the vehicle CSV
        static final int COL_CAR = 0;  // vehicle ID
        static final int COL_DAT = 1;  // date in YYMMDD
        static final int COL_TIM = 2;  // time in HHMMSS
        static final int COL_LON = 3;  // longitude in DMS
        static final int COL_LAT = 4;  // latitude in DMS
        static final int COL_DIR = 5;  // compass orientation in degrees
        static final int COL_SPD = 6;  // speed in km/h
        static final int COL_ROD = 7;  // road type code
    
        @Override
        public void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
    
            /* 
             * The TextInputFormat we set in the configuration, splits a text file line by line.
             * The key is the byte offset to the first character in the line.  The value is the text of the line.
             * Note: no header row in this CSV.
             */
    
            String line = val.toString();
            String[] values = line.split(",");  // no comma inside string in input
            Text key2 = new Text(values[COL_CAR] + "," + values[COL_DAT]);
            CarSortWritable data = new CarSortWritable(values[COL_DAT], values[COL_TIM],
                                                       values[COL_LON], values[COL_LAT],
                                                       values[COL_DIR], values[COL_SPD], values[COL_ROD]);
            context.write(key2, data);
    
        }
    
    }
    

In order to support trips that span midnight - when in possession of multiple
days of data - a mapper could use a key consisting of the car ID only -
passing to the reducer, a potentially longer list of position records, which
would need to be sorted by date and time.  

The grid of cells to cover Japan, is calculated in the setup of the reducer of
the first MapReduce application - once per reducer node.  If we had simply
used an equal-angle grid with the geographic coordinates, the geodesic area of
a cell would have differed by about 18% between northernmost and southernmost
Japan - so we generate an equal-area grid, while still using geographic
coordinates. To produce an equal-area grid, the code takes the envelope of the
study area, and uses the middle latitude as a baseline.  It uses
`GeometryEngine.geodesicDistanceOnWGS84` to calculate the length of a 1° arc
along the latitude (using WGS84 to make a close approximation of distance -
not position - on Tokyo GCS), then uses proportions to find the X-axis angle
corresponding to the desired cell width.  With a constant X-axis angle, it
calculates variable Y-axis grid angles, starting at the southernmost latitude
and working northward, using `GeometryEngine.geodesicDistanceOnWGS84` on the
X-axis angle cell width, and dividing into the constant area to get the Y-axis
angle for each row of the grid.  

    
    
        private void buildGrid(double gridSide) {   // Nominal length of side of grid cell (meters)
            double cellArea = gridSide*gridSide;
            latMax = envelope.getYMax() + .005;  // +.005 to catch nearby outliers (approx. 500m)
            latMin = envelope.getYMin() - .005;  // -.005 to catch nearby outliers
            final double latMid = (latMax + latMin) / 2;
            latExtent = latMax-latMin;
            lonMin = envelope.getXMin() - .005;  // -.005 to catch nearby outliers
            lonMax = envelope.getXMax() + .005;  // +.005 to catch nearby outliers
            final double lonOneDeg = lonMin + 1;  // arbitrary longitude for establishing lenOneDegBaseline
            Point fromPt = new Point(lonMin, latMid),
                toPt = new Point(lonOneDeg, latMid);
            // geodesicDistanceOnWGS84 is an approximation as we are using a different GCS, but expect it
            // to be a good approximation as we are using proportions only, not positions, with it.
            final double lenOneDegBaseline = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
            // GeometryEngine.distance "Calculates the 2D planar distance between two geometries".
            // angle: GeometryEngine.distance(fromPt, toPt, spatialReference);
            arcLon = gridSide / lenOneDegBaseline;  // longitude arc of grid cell
            final double latOneDeg = latMid + 1;
            toPt.setXY(lonMin, latOneDeg);
            final double htOneDeg = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
    
            int enough = (int)(Math.ceil(.000001 + (lonMax-lonMin)*lenOneDegBaseline/gridSide)) *
                         (int)(Math.ceil(.000001 + latExtent*htOneDeg/gridSide)) ;
            grid = new ArrayList<double[]>(enough);
            double xlon, ylat;
            // Could filter out cells that do not overlap country polygon (if using quadtree).
            for (ylat = latMin, yCount = 0;  ylat < latMax;  yCount++) {
                fromPt.setXY(lonMin, ylat);
                toPt.setXY(lonMin+arcLon, ylat);
                double xlen = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
                double height = cellArea/xlen;  // meters
                double arcLat = height / htOneDeg;
                for (xlon = lonMin, xCount = 0;  xlon < lonMax;  xlon += arcLon, xCount++) {
                    double[] tmp = {xlon, ylat, xlon+arcLon, ylat+arcLat};
                    grid.add(tmp);
                }
                ylat += arcLat;
            }
        }
    
        @Override
        public void setup(Context context) {
            // First pull values from the configuration     
            Configuration config = context.getConfiguration();
    
            int minutes = config.getInt("com.esri.trip.threshold", 15);  //minutes stoppage delineating trips
            threshold = minutes * 60;  // minutes -> seconds
    
            double gridSide = 1000.;   // Nominal/average/target length of side of grid cell (meters)
            String sizeArg = config.get("com.esri.trip.cellsize", "1000");
            if (sizeArg.length() > 0 && sizeArg.charAt(0) != '-') {
                double trySize = Double.parseDouble(sizeArg);
                if (trySize >= 100)  //likely unrealistic smaller than about 200m to 500m
                    gridSide = trySize;  // input as meters
                else if (trySize > 0)
                    gridSide = 1000 * trySize;  // input as km
            }
    
            String featuresPath = config.get("com.esri.trip.input");
            FSDataInputStream iStream = null;
            spatialReference = SpatialReference.create(4301);  //  GCS_Tokyo
    
            try {
                // load the JSON file provided as argument
                FileSystem hdfs = FileSystem.get(config);
                iStream = hdfs.open(new Path(featuresPath));
                country = EsriFeatureClass.fromJson(iStream);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (iStream != null) {
                    try {
                        iStream.close();
                    } catch (IOException e) { }
                }
            }
    
            // build the grid of cells
            if (country != null) {
                envelope = new Envelope();
                country.features[0].geometry.queryEnvelope(envelope);
                buildGrid(gridSide);
            }
        }
    

Finding the cell containing a location point, when the cells are stored as an
array of bounds, and each row has the same number of cells, proceeds as
follows.  The X-axis index is a straightforward division by cell width.  The
Y-axis calculation is adjusted for possible overshoot or undershoot, due to
varying cell height.  Then `cellIndex = xIndex + xCount * yIndex`.  

    
    
        private int queryGrid(double longitude, double latitude) {
            int cellIndex; // xIdx + xCount * yIdx
            if (longitude >= lonMin && longitude <= lonMax  &&
                latitude >= latMin  && latitude <= latMax)  {   // avoid outliers
                int xIdx = (int)((longitude-lonMin)/arcLon);
                if (xIdx >= 0 && xIdx < xCount) {
                    // Initial quotient is approximate, to refine
                    int yIdx = (int)(yCount*(latitude-latMin)/latExtent);
                    yIdx = yIdx < yCount ? yIdx : yCount - 1;
                    cellIndex = xIdx + xCount * yIdx;
                    // Expect either correct, or one of either too high or too low, not both
                    while (grid.get(cellIndex)[1] > latitude) {   // bottom too high
                        yIdx--;
                        cellIndex -= xCount;
                    }
                    while (grid.get(cellIndex)[3] < latitude) {   // top too low
                        yIdx++;
                        cellIndex += xCount;
                    }
                    if (yIdx < 0 || yIdx >= yCount) {  // bug
                        cellIndex = -3;
                    }
                } else {  // bug
                    cellIndex = -2;
                }
            } else {  // outlier
                cellIndex = -1;
            }
            return cellIndex;
        }
    

Inferred trips were calculated as follows.  The GPS units in the cars transmit
a point of position data about every 30 seconds (with some variation), while
the car is on. For successive positions for the same car, the code looks for
lapses in the position points. For each lapse of more than 15 minutes, it
interprets the position before the lapse as the destination of a trip, and the
position after the lapse as the origin of a new trip. Also, the last position
of the car in the day, is interpreted as the destination of a trip (except in
the case of a lone position point).  

    
    
        public void reduce(Text key, Iterable<CarSortWritable> values, Context ctx)
            throws IOException, InterruptedException {
    
            String[] kys = key.toString().split(",");  // carID & date
            Text outKy = new Text(kys[0]);
    
            // Expect at most tens of thousands of positions per car per day - expect up to thousands.
            // (per year, up to 2-3 hundreds of thousands)
            final int MAX_BUFFER_SIZE = 8000;  // would fit a record every 11s all day
            ArrayList<CarSortWritable> records = new ArrayList<CarSortWritable>(MAX_BUFFER_SIZE);
    
            for (CarSortWritable entry : values) {
                records.add(new CarSortWritable(entry));
            }
            Collections.sort(records);
    
            // Keep origin & last/previous time & position
            CarSortWritable first = records.get(0);
            String theDate = first.getDate(), origTime = first.getTime(),
                origLon = first.getLon(), origLat = first.getLat(), origSpd = first.getSpeed(),
                prevTime = null, prevLon = null, prevLat = null, prevSpd = null;
            long nOrgTm = timeAsInteger(theDate, origTime), nPrvTm = -1;
            try {               // Check if lapse exceeding threshold.
                // The check for time lapse, without checking position movement,
                // utilizes the fact that these GPS units transmit data only
                // when the car is on - or at least do not transmit data when
                // the key is altogether out of the ignition.
                for (CarSortWritable entry : records) {
                    String curTime = entry.getTime(), curLon = entry.getLon(),
                        curLat = entry.getLat(), curSpd = entry.getSpeed();
                    long nCurTm = timeAsInteger(theDate, curTime);
    
                    if (nPrvTm > nOrgTm   //ignore lone points
                        && nCurTm > nPrvTm + threshold) {
    
                        int idxOrig = queryGrid(DegreeMinuteSecondUtility.parseDms(origLon),
                                                DegreeMinuteSecondUtility.parseDms(origLat));
                        int idxDest = queryGrid(DegreeMinuteSecondUtility.parseDms(prevLon),
                                                DegreeMinuteSecondUtility.parseDms(prevLat));
                        if (idxOrig >= 0 && idxDest > 0) {  // discard outliers
                            double[] cellOrig = grid.get(idxOrig);
                            double[] cellDest = grid.get(idxDest);
                            ctx.write(outKy,
                                      new TripCellWritable(theDate, origTime, origLon, origLat, origSpd,
                                                           cellOrig[0], cellOrig[1], cellOrig[2], cellOrig[3],
                                                           theDate, prevTime, prevLon, prevLat, prevSpd,
                                                           cellDest[0], cellDest[1], cellDest[2], cellDest[3]));
                        }
                        nOrgTm   = nCurTm;
                        origTime = curTime;
                        origLon  = curLon;
                        origLat  = curLat;
                        origSpd  = curSpd;
                    }
                    nPrvTm   = nCurTm;
                    prevTime = curTime;
                    prevLon  = curLon;
                    prevLat  = curLat;
                    prevSpd  = curSpd;
                }
    
                if (/*records.size() > 1 && */ nPrvTm > nOrgTm) {  // no lone point
                    int idxOrig = queryGrid(DegreeMinuteSecondUtility.parseDms(origLon),
                                            DegreeMinuteSecondUtility.parseDms(origLat));
                    int idxDest = queryGrid(DegreeMinuteSecondUtility.parseDms(prevLon),
                                            DegreeMinuteSecondUtility.parseDms(prevLat));
                    if (idxOrig >= 0 && idxDest > 0) {  // discard outliers
                        double[] cellOrig = grid.get(idxOrig);
                        double[] cellDest = grid.get(idxDest);
                        ctx.write(outKy,
                                  new TripCellWritable(theDate, origTime, origLon, origLat, origSpd,
                                                       cellOrig[0], cellOrig[1], cellOrig[2], cellOrig[3],
                                                       theDate, prevTime, prevLon, prevLat, prevSpd,
                                                       cellDest[0], cellDest[1], cellDest[2], cellDest[3]));
                    }
                }
            } catch (Exception e) {
                // could log something
            }
        }
    

This trip-inference computation relies on the fact that the car GPS unit does
not transmit data when the car is off. For trip discovery on data from GPS
units that continue transmitting while the vehicle is off, it would be
necessarily to additionally check whether the car has in fact moved more than
a threshold roaming distance (using `GeometryEngine.geodesicDistanceOnWGS84`).


The input for the second-stage MapReduce job, was the output of the first
stage, namely the longitude, latitude, and grid-cell bounds of both the origin
and destination of the trips. The second-stage MapReduce job grouped the
inferred trips by origin cell, in the mapper. Then for each origin cell, the
reducer counted trips by grid cell containing the destination of the trip, to
determine - for that origin cell - the number of trips ending in the most-
common destination cell.

    
    
        public void reduce(Text key, Iterable<TripInCommonWritable> values, Context ctx)
            throws IOException, InterruptedException {
    
            final int INIT_SIZE = 8000;
            HashMap<String,Long> records = new HashMap<String,Long>(INIT_SIZE);
    
            String sval,  // destination cell bounds - iterator value, hashmap key
                   maxDest = null;  // most common destination cell (bounds)
            long totCount = 0, maxCount = 0;
            for (TripInCommonWritable entry : values) {
                sval = entry.toString();
                long newCount = records.containsKey(sval) ? 1 + records.get(sval) : 1;
                records.put(sval, newCount);
                if (newCount > maxCount) {
                    maxCount = newCount;
                    maxDest = sval;
                }
                totCount++;
            }  // /for
            Configuration config = ctx.getConfiguration();
            // minimum count per cell, for inclusion in output
            int minPoints = config.getInt("com.esri.trip.threshold", 10);
            minPoints = minPoints < 2 ? 1 : minPoints;
            if (totCount >= minPoints) {
                double pct = 0.;   // If only one trip going to each destination cell, report 0% in common.
                if (maxCount > 1)
                    pct = 100. * (double)maxCount / (double)totCount;
                ctx.write(key, new Text(String.format("%d\t%d\t%f\t%s",
                                                      totCount, maxCount, pct,
                                                      maxDest)));
            }
        }
    

To execute the calculations, the MapReduce jobs can be invoked by using either
the _Execute Workflow_ tool in ArcMap, or the command line.  Here are recipes
for invoking the MapReduce jobs from command line:

    
    
    env HADOOP_CLASSPATH=../lib/esri-geometry-api.jar:../lib/spatial-sdk-hadoop.jar \
      hadoop jar trip-discovery.jar com.esri.hadoop.examples.trip.TripCellDriver \
      -libjars ../lib/esri-geometry-api.jar,../lib/spatial-sdk-hadoop.jar \
      15 500 trips/japan-tokyo-gcs.json trips/vehicle-positions.csv \
      out-trips-inferred
    
    env HADOOP_CLASSPATH=../lib/esri-geometry-api.jar \
      hadoop jar trip-discovery.jar com.esri.hadoop.examples.trip.TripInCommonDriver \
      -libjars ../lib/esri-geometry-api.jar \
      1 'out-trips-inferred/part-r-*' out-trips-by-origin-cell
    

The tab-separated output was converted to JSON - a format suitable for import
to ArcMap - with Hive queries using ST_Geometry functions from GIS Tools for
Hadoop.

    
    
    create external table trips_by_origin_cell(leftlon double, botlat double, rightlon double, toplat double,
                                               totnum int, samedest int, pct double,
                                               destlhs double, destbot double, destrhs double, desttop double)
    row format delimited fields terminated by '\t'
    location '/user/rwhitman/out-trips-by-origin-cell';
    
    create external table trip_origin_json  (totnum int, samedest int, pct double, destlhs double, destbot double,
                                             destrhs double, desttop double, shape binary)
    row format serde 'com.esri.hadoop.hive.serde.JsonSerde'
    stored as inputformat 'com.esri.json.hadoop.UnenclosedJsonInputFormat'
    outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/user/rwhitman/trip-origin-json';
    
    insert overwrite table trip_origin_json select totnum, samedest, pct, destlhs, destbot, destrhs, desttop,
      ST_Polygon(leftlon,botlat, rightlon,botlat, rightlon,toplat, leftlon,toplat) from trips_by_origin_cell;
    

Next, the JSON file of results, was copied from HDFS, to be accessible to
ArcMap.  This can be done either with command-line `hadoop` `fs`, or by using
the _Copy From HDFS_ tool.

![[GP Tool: Copy From HDFS]](copy-from-hdfs-trips-arcmap.png)

The tool expects the following parameters:

  * The HDFS parameters for hostname, port, and username - these relate to the configuration of Hadoop - same as with the Copy To HDFS tool.
  * The HDFS remote file - is the directory in HDFS generated by the previous step - it contains a JSON file.  

  * The Output local file parameter is a filename of our choosing, with `.json` extension.  

Then the results were imported to ArcMap as a feature class, by using the
_JSON To Features_ tool.

![[GP tool: JSON To Features]](json-features-trips-arcmap.png)

The Input JSON is the file copied over in the previous step.  The Output
feature class is a new ArcGIS feature class, with a name of our choosing.  For
JSON type, be sure to select UNENCLOSED_JSON.

As the unenclosed JSON that was imported does not carry information as to
spatial reference, it is necessary to right click the newly-imported feature
class in the Catalog - then Properties, XY Coordinate System, Geographic
Coordinate Systems, Asia, Tokyo.

![[Feature Class Properties: Spatial Reference]](spatial-reference-arcmap.png)  

Finally, we used ArcMap to visualize the results in a map. The feature class
imported from JSON, has an integer field for the count of the trips from each
origin cell, that ended in the most common destination cell. In ArcMap, it is
possible to use this integer-value field for symbols varying by quantity. The
map shows cells that were the origin of five or more trips to a common
destination cell. Cells that were the origin of 11 or trips to a common
destination, are symbolized with bigger and darker purple squares, to
highlight candidate areas for carpool suggestions.

![Map: by origin cell, count of car trips to common destination cell](cars-jp20b.png)  

A potential further study could additionally consider the following:

  * The location and distance of the most-common destination cell, for each origin cell of interest;
  * Time slices, as many people need to go to their destination during a specific part of the day.

The following open-source projects, from GIS Tools for Hadoop on Github, were
used:

  * The [geometry-api-java library](https://github.com/Esri/geometry-api-java) was used for computing equal-area grid cells.
  * From [spatial-framework-for-hadoop](https://github.com/Esri/spatial-framework-for-hadoop), Hive UDFs were used to create geometries from raw data, along with API utilities to export and import data to and from JSON.
  * Geoprocessing tools were used from [geoprocessing-tools-for-hadoop](https://github.com/Esri/geoprocessing-tools-for-hadoop), to import results into ArcMap for visualization, as well as to export a polygon of Japan for gridding.  

The MapReduce calculations ran in under an hour on a single-node development
instance of Hadoop. This provides a proof of concept of using a Hadoop cluster
to run similar calculations on much bigger data sets.

The complete source code is [available on
Github](https://github.com/randallwhitman/gis-tools-for-hadoop).  

