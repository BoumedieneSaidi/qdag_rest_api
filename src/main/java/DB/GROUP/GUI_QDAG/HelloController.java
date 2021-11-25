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
    @GetMapping("/run-query")
    public Map<String, String> sendQuery(@RequestParam("db") String db,@RequestParam("queryPath") String queryPath,@RequestParam("resultFile") String resultFile) throws IOException,InterruptedException{
        File dbDir = new File("/home/boumi/RDF_QDAG_TEST_DIR/"+ db);
        File queryFile = new File("/home/boumi/RDF_QDAG_TEST_DIR/watdiv/gStore/"+ queryPath);
        if (!dbDir.exists() || !queryFile.exists()) 
           return new HashMap<>();
        Process proc = Runtime.getRuntime().exec("java -jar /home/boumi/querySender.jar "+db+" "+queryPath+" "+resultFile);
        proc.waitFor();
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(new FileInputStream("/home/boumi/queryResults/"+resultFile)));
        String resultLine = null;
        String finalResult = "";
        String execTime = "";
        int i = 0,maxPage = 10;
        Path path = Paths.get("/home/boumi/queryResults/"+resultFile);
        int nb = (int)(Files.lines(path).count() - 1);
        while (i < (nb - 1) && (resultLine  = stdInput.readLine()) != null && i < maxPage){
            if(i < maxPage - 1)
                finalResult += resultLine  + "\n";
           else
                finalResult += resultLine;
           execTime = resultLine;
           i++;
        }
        Map<String, String> result = new HashMap<>();
        result.put("finalResult",finalResult);
        result.put("execTime",execTime);
        result.put("nbrRes",""+(Files.lines(path).count() - 1));
		return result;
    }
    @GetMapping("/fetch-data")
    public Map<String, String> fetchData(@RequestParam("page") String page,@RequestParam("perPage") String perPage,@RequestParam("resultFile") String resultFile) throws IOException,InterruptedException{
        String res = Files.lines(Paths.get("/home/boumi/queryResults/"+resultFile)).skip((Integer.valueOf(page) - 1) * Integer.valueOf(perPage)).
        limit(Integer.valueOf(perPage)).collect(Collectors.joining ("\n"));;
        Map<String, String> result = new HashMap<>();
        result.put("finalResult",res);
        result.put("nbrRes",""+(Files.lines(Paths.get("/home/boumi/queryResults/"+resultFile)).count() - 1));
		return result;
    }
}
