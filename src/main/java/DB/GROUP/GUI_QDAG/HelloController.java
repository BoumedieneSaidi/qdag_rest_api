package DB.GROUP.GUI_QDAG;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.nio.file.Files;
import java.util.stream.Collectors;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class HelloController {
    public static final String MAIN_DIR = System.getProperty("user.home") + "/demo_edbt_deploy/";
    public static final String RESULTS_DIR_PATH = MAIN_DIR + "results_dir/";
    public static final String DBS_PATH = MAIN_DIR + "databases/";//System.getProperty("user.home") + "/" +"RDF_QDAG_TEST_DIR/";
    public static final String QUERIES_PATH = MAIN_DIR + "queries/";//System.getProperty("user.home") + "/" +"RDF_QDAG_TEST_DIR/watdiv/gStore/";
    public static final int PAGE_SIZE = 10;
    private Map<String,String> map = new HashMap<>();
    private Map<String,String> mapBDD = new HashMap<>();
    private Map<String,String> mapBDDVirtuoso = new HashMap<>();
    private Map<String,String> rdfResult = new HashMap<>();
    private Map<String,String> virtuosoResult = new HashMap<>();
    private Map<String,Integer> instancesPorts = new HashMap<>();
    public HelloController(){
        mapDBSName();
        initRdfResult();
        initVirtuosoResult();
        mapQueriesName();
        //créer le répertoir des résultats dés que le serveur est lancée
        createResultsDir();
        //Lancer les instances
        launchInstances();
    }
    private void createResultsDir(){
        File directory = new File(RESULTS_DIR_PATH);
        if (!directory.exists())
            directory.mkdir();
    }
    @GetMapping("/run-query")
    public Map<String, String> runQuery(@RequestParam("db") String dbName,@RequestParam("queryPath") String queryName,@RequestParam("resultFile") String resultFileName
    ,@RequestParam("optimizer") String optimizer,@RequestParam("isPrun") String isPrun) throws IOException,InterruptedException{        
        Map<String, String> result = new HashMap<>();
        String resultFilePath = RESULTS_DIR_PATH +  resultFileName;
        int port = instancesPorts.get(mapBDD.get(dbName));
        Process proc = Runtime.getRuntime().exec("java -jar "+MAIN_DIR+"qdag_jars/querySender.jar "+ 
        port+" "+(QUERIES_PATH + map.get(queryName))+" "+(optimizer.equals("Heuristics") ? "heuristics": "gofast")+" "+isPrun+" "+resultFilePath);
        System.out.println("java -jar "+MAIN_DIR+"qdag_jars/querySender.jar "+ 
        port+" "+(QUERIES_PATH + map.get(queryName))+" "+(optimizer.equals("Heuristics") ? "heuristics": "gofast")+" "+isPrun+" "+resultFilePath);
        BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
            if(line.equals("error")){
                 return result;
            }
        }
        proc.waitFor();//wait for process to finish
        //Thread.sleep(4000);//should find a solution about that
        Path pathToResultFile = Paths.get(resultFilePath);
        //get result number
        String resultStr = "";
        int nbrResults = (int)(Files.lines(pathToResultFile).count() - 1);
        if(nbrResults != 0)
                resultStr = Files.lines(pathToResultFile).limit(PAGE_SIZE).collect(Collectors.joining ("\n"));
        String execTime = Files.lines(pathToResultFile).skip(nbrResults).findFirst().get();
        //retourner les résultat sous format json en utilisant le map
        
        result.put("finalResult",nbrResults == 0 ? "":resultStr);
        result.put("execTime",execTime);
        result.put("nbrRes",""+nbrResults);
		return result;
    }
    @GetMapping("/fetch-data")
    public Map<String, String> fetchData(@RequestParam("page") int page,@RequestParam("perPage") int perPage,@RequestParam("resultFile") String resultFileName) throws IOException,InterruptedException{
        String resultFilePath = RESULTS_DIR_PATH +  resultFileName;
        String resultStr = Files.lines(Paths.get(resultFilePath)).skip((page - 1) * perPage).
                            limit(perPage).collect(Collectors.joining ("\n"));
        Map<String, String> result = new HashMap<>();
        result.put("finalResult",resultStr);
        result.put("nbrRes",""+(Files.lines(Paths.get(resultFilePath)).count() - 1));
		return result;
    }
    @GetMapping("/run-rdf3x")
    public Map<String, String> runRDF3X(@RequestParam("db") String db,@RequestParam("query") String query) throws IOException,InterruptedException{
        /*ProcessBuilder builder = new ProcessBuilder("/home/boumi/gh-rdf3x/bin/rdf3xquery","/home/boumi/gh-rdf3x/bin/"+"watdiv100k",QUERIES_PATH +query);
        builder.redirectOutput(new File("/home/boumi/out.txt"));
        builder.redirectError(new File("/home/boumi/out.txt"));
        long t1 = System.currentTimeMillis();
        Process proc = builder.start();
        proc.waitFor();
        long rdfExecTime = System.currentTimeMillis() - t1;*/
        Map<String, String> result = new HashMap<>();
        result.put("rdfExecTime",""+(rdfResult.get(query)));
		return result;
    }
    @GetMapping("/run-virtuoso")
    public Map<String, String> runVirtuoso(@RequestParam("db") String db,@RequestParam("query") String query) throws IOException,InterruptedException{
        /*ProcessBuilder builder = new ProcessBuilder("/home/boumi/gh-rdf3x/bin/rdf3xquery","/home/boumi/gh-rdf3x/bin/"+"watdiv100k",QUERIES_PATH +query);
        builder.redirectOutput(new File("/home/boumi/out.txt"));
        builder.redirectError(new File("/home/boumi/out.txt"));
        long t1 = System.currentTimeMillis();
        Process proc = builder.start();
        proc.waitFor();
        long rdfExecTime = System.currentTimeMillis() - t1;*/
       // Process proc = Runtime.getRuntime().exec("docker exec -it virtuoso-watdiv100m isql-v -U dba -P myDbaPassword  exec=\"load C1.in;\"");
       if(db.equals("Yago")){
           Map<String, String> result = new HashMap<>();
           //System.out.println("Olaaa:"+query+", result:"+)
            result.put("virtuosoExecTime", virtuosoResult.get(query));
            return result;
       }else{
        String a="load "+map.get(query)+";";
        System.out.println("Db:"+mapBDDVirtuoso.get(db));
        ProcessBuilder builder = new ProcessBuilder("docker","exec","-i",mapBDDVirtuoso.get(db),"isql-v","-U","dba","-P","myDbaPassword","exec="+a);
        builder.redirectOutput(new File("/home/boumi/out.txt"));
        builder.redirectError(new File("/home/boumi/out.txt"));
         
        Process proc = builder.start();
        proc.waitFor();

       
       
        String lastLine = "",sCurrentLine ="";
        BufferedReader br = new BufferedReader(new FileReader("/home/boumi/out.txt"));
        while ((sCurrentLine = br.readLine()) != null)
            lastLine = sCurrentLine;
        Map<String, String> result = new HashMap<>();
        result.put("virtuosoExecTime",findIntegers(lastLine).get(1) + "");
        ProcessBuilder dockerRestarter = new ProcessBuilder("docker","restart",mapBDDVirtuoso.get(db));
        /*dockerRestarter.redirectOutput(new File("/home/boumi/out.txt"));
        dockerRestarter.redirectError(new File("/home/boumi/out.txt"));*/
         
        Process procRestarter = dockerRestarter.start();
        procRestarter.waitFor();
		return result;}
    }
    private void initRdfResult(){
        /*rdfResult.put("Complex 1","2200");rdfResult.put("Complex 2","5070");rdfResult.put("Complex 3","20757");
        rdfResult.put("SnowFlake-Shaped_1","1670"); rdfResult.put("SnowFlake-Shaped_2","1780");rdfResult.put("SnowFlake-Shaped_3","1690") ;   
        rdfResult.put("Linear 1","33")  ;rdfResult.put("Linear 2","560") ; rdfResult.put("Linear 3","1910")  ;
        rdfResult.put("Star 1","1408")   ;rdfResult.put("Star 2","163")  ;rdfResult.put("Star 3","42
        ") ;
        /*rdfResult.put("Yago 1","y1.in")  ;rdfResult.put("Yago 2","yz.in")  ;rdfResult.put("Yago 3","y3.in")  ;rdfResult.put("Yago 4","y4.in")  ;
        rdfResult.put("Yago 5","y5.in")  ;rdfResult.put("Yago 6","y6.in")  ;rdfResult.put("Yago 7","y7.in")  ;rdfResult.put("Yago 8","y8.in")  ;*/
    }
    private void initVirtuosoResult(){
        virtuosoResult.put("Yago 1","25016")  ;virtuosoResult.put("Yago 2","20079")  ;
        //virtuosoResult.put("Yago 3","y3.in")  ;virtuosoResult.put("Yago 4","y4.in")  ;
        //virtuosoResult.put("Yago 5","y5.in")  ;virtuosoResult.put("Yago 6","y6.in")  ;virtuosoResult.put("Yago 7","y7.in")  ;virtuosoResult.put("Yago 8","y8.in")  ;
    }
    private void mapQueriesName(){
        map.put("Wild Card 1","WC1.in");map.put("Wild Card 2","WC2.in");map.put("Wild Card 3","WC3.in");
        map.put("Complex 1","C1.in");map.put("Complex 2","C2.in");map.put("Complex 3","C3.in");          
        map.put("SnowFlake-Shaped_1","F2.in"); map.put("SnowFlake-Shaped_2","F3.in");map.put("SnowFlake-Shaped_3","F4.in") ;        
        map.put("Star 1","S1.in");map.put("Star 2","S2.in");map.put("Star 3","S6.in") ; 
        map.put("Linear 1","L1.in")  ;map.put("Linear 2","L2.in") ; map.put("Linear 3","L4.in")  ;
        map.put("Grouping 1","G1.in") ; map.put("Grouping 2","G2.in")  ;map.put("Grouping 3","G3.in")  ;map.put("Grouping 4","G4.in")  ;
        map.put("Sorting 1","O1.in")  ;map.put("Sorting 2","O2.in")  ;map.put("Sorting 3","O3.in")  ;map.put("Sorting 4","O4.in")  ;
        map.put("Yago 1","y1.in")  ;map.put("Yago 2","yz.in")  ;map.put("Yago 3","y3.in")  ;map.put("Yago 4","y4.in")  ;
        map.put("Yago 5","y5.in")  ;map.put("Yago 6","y6.in")  ;map.put("Yago 7","y7.in")  ;map.put("Yago 8","y8.in")  ;
    }
    private void mapDBSName(){
        mapBDD.put("Watdiv100M","watdiv100m_bin");
        mapBDD.put("Watdiv1B","watdiv1000m_bin");
        mapBDD.put("Yago","YAGO_BASE_final_bin2");
        
        mapBDDVirtuoso.put("Yago","virtuoso-yago");
        mapBDDVirtuoso.put("Watdiv100M","virtuoso-watdiv100m");
        //mapBDDVirtuoso.put("Watdiv1000m","virtuoso-watdiv1000m");

        instancesPorts.put("watdiv100m_bin",64000);
        //instancesPorts.put("watdiv1000m_bin",64001);
        instancesPorts.put("YAGO_BASE_final_bin2",64002);
        /*mapBDD.put("watdiv100k","watdiv100k");
        instancesPorts.put("watdiv100k",64000);*/
    }
    private void launchInstances(){
        instancesPorts.forEach((db,port)-> {
            try {
                ProcessBuilder builder = new ProcessBuilder("numactl","--physcpubind=+1","java","-jar","-Djava.library.path="+MAIN_DIR+"solibs","-Xms4g","-Xmx16g",MAIN_DIR+"qdag_jars/instance.jar",
                MAIN_DIR+"databases/"+db,""+port);
                System.out.println("numactl"+" --physcpubind=+1"+" java"+" -jar"+" -Djava.library.path="+MAIN_DIR+" solibs"+" -Xms4g"+" -Xmx16g "+MAIN_DIR+" qdag_jars/instance.jar "+
                MAIN_DIR+"databases/"+db+" "+port);
                //Process proc = Runtime.getRuntime().exec("numactl --physcpubind=+1 java -jar -Djava.library.path="+MAIN_DIR+"solibs -Xms4g -Xmx16g "+MAIN_DIR+"qdag_jars/instance.jar "
                //+MAIN_DIR+"databases/"+db+" "+port+"  &> /home/boumi/qdag_log");
                File log = new File(MAIN_DIR+"/log/"+db);
                builder.redirectErrorStream(true);
                builder.redirectOutput(Redirect.appendTo(log));
                Process process = builder.start();
            } catch (IOException e) { e.printStackTrace();}
        });
        
    }
    List<String> findIntegers(String stringToSearch) {
        Pattern integerPattern = Pattern.compile("-?\\d+");
        Matcher matcher = integerPattern.matcher(stringToSearch);

        List<String> integerList = new ArrayList<>();
        while (matcher.find()) {
            integerList.add(matcher.group());
        }
        return integerList;
    }
}
