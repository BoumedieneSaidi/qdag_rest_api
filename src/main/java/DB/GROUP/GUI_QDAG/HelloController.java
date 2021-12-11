package DB.GROUP.GUI_QDAG;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.nio.file.Files;
import java.util.stream.Collectors;
import java.io.*;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.core.io.ClassPathResource;
@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class HelloController {
    public static final String RESULTS_DIR_PATH = System.getProperty("user.home") + "/" +"results_dir/";
    public static final String DBS_PATH = System.getProperty("user.home") + "/" +"data/";//System.getProperty("user.home") + "/" +"RDF_QDAG_TEST_DIR/";
    public static final String QUERIES_PATH = System.getProperty("user.home")+ "/queries/";//System.getProperty("user.home") + "/" +"RDF_QDAG_TEST_DIR/watdiv/gStore/";
    public static final int PAGE_SIZE = 10;
    private Map<String,String> map = new HashMap<>();
    private Map<String,String> mapBDD = new HashMap<>();
    private Map<String,String> rdfResult = new HashMap<>();
    public HelloController(){
        mapBDD.put("Watdiv100m","watdiv100m_bin");
        mapBDD.put("Yago","YAGO_BASE_final_bin2");
        rdfResult.put("Complex 1","12920");
        rdfResult.put("Complex 2","660");
        rdfResult.put("Complex 3","66110");
        rdfResult.put("SnowFlake-Shaped_1","730"); 
        rdfResult.put("SnowFlake-Shaped_2","780");
        rdfResult.put("SnowFlake-Shaped_3","1460") ;   
        rdfResult.put("Linear 1","60")  ;
        rdfResult.put("Linear 2","1090") ; 
        rdfResult.put("Linear 3","750")  ;

        rdfResult.put("Star 1","2010")   ;
        rdfResult.put("Star 2","280")  ;
        rdfResult.put("Star 3","920") ;

        map.put("Wild Card 1","WC1.in");
        map.put("Wild Card 2","WC2.in");
        map.put("Wild Card 3","WC3.in");
        map.put("Complex 1","C1.in");
        map.put("Complex 2","C2.in");           
        map.put("Complex 3","C3.in");          
        map.put("SnowFlake-Shaped_1","F2.in"); 
        map.put("SnowFlake-Shaped_2","F3.in");
        map.put("SnowFlake-Shaped_3","F4.in") ;        
        map.put("Star 1","S1.in")   ;
        map.put("Star 2","S2.in")  ;
        map.put("Star 3","S6.in") ; 
        map.put("Linear 1","L1.in")  ;
        map.put("Linear 2","L2.in") ; 
        map.put("Linear 3","L4.in")  ;
        map.put("Grouping 1","G1.in") ; 
        map.put("Grouping 2","G2.in")  ;
        map.put("Grouping 3","G3.in")  ;
        map.put("Grouping 4","G4.in")  ;
        map.put("Sorting 1","O1.in")  ;
        map.put("Sorting 2","O2.in")  ;
        map.put("Sorting 3","O3.in")  ;
        map.put("Sorting 4","O4.in")  ;
        map.put("RDF First","y1.in")  ;
        map.put("Spatial First","yz.in")  ;
        //créer le répertoir des résultats dés que le serveur est lancée
        createResultsDir();
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
        Process proc = Runtime.getRuntime().exec("java -jar /home/boumi/querySender.jar "+ 
        (DBS_PATH + mapBDD.get(dbName))+" "+(QUERIES_PATH + map.get(queryName))+" "+(resultFilePath)+" "+(optimizer.equals("Heuristics") ? "heuristics": "gofast")+" "+isPrun);
        BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
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
}
