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
    public static final String DBS_PATH = System.getProperty("user.home") + "/" +"RDF_QDAG_TEST_DIR/";
    public static final String QUERIES_PATH = System.getProperty("user.home") + "/" +"RDF_QDAG_TEST_DIR/watdiv/gStore/";
    public static final int PAGE_SIZE = 10;
    public HelloController(){
        //créer le répertoir des résultats dés que le serveur est lancée
        createResultsDir();
    }
    private void createResultsDir(){
        File directory = new File(RESULTS_DIR_PATH);
        if (!directory.exists())
            directory.mkdir();
    }
    @GetMapping("/run-query")
    public Map<String, String> runQuery(@RequestParam("db") String dbName,@RequestParam("queryPath") String queryName,@RequestParam("resultFile") String resultFileName) throws IOException,InterruptedException{        
        String resultFilePath = RESULTS_DIR_PATH +  resultFileName;
        Process proc = Runtime.getRuntime().exec("java -jar /home/boumi/querySender.jar "+ 
        (DBS_PATH + dbName)+" "+(QUERIES_PATH + queryName)+" "+(resultFilePath));
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
        Map<String, String> result = new HashMap<>();
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
        ProcessBuilder builder = new ProcessBuilder("/home/boumi/gh-rdf3x/bin/rdf3xquery","/home/boumi/gh-rdf3x/bin/"+"watdiv100k",QUERIES_PATH +query);
        builder.redirectOutput(new File("/home/boumi/out.txt"));
        builder.redirectError(new File("/home/boumi/out.txt"));
        long t1 = System.currentTimeMillis();
        Process proc = builder.start();
        proc.waitFor();
        long rdfExecTime = System.currentTimeMillis() - t1;
        Map<String, String> result = new HashMap<>();
        result.put("rdfExecTime",""+rdfExecTime);
		return result;
    }
}
