package DB.GROUP.GUI_QDAG;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;
import java.util.*;
import java.nio.file.Files;
import java.io.*;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.core.io.ClassPathResource;
@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class HelloController {
	@GetMapping("/ru-query")
	public String runQuery(@RequestParam("query") String query,@RequestParam("db") String db) throws IOException {
        Process proc = Runtime.getRuntime().exec("java -jar -Djava.library.path=/home/ubuntu/p_qdag/solibs/ /home/ubuntu/p_qdag/"+
        "control_scripts_local/gquery.jar /home/ubuntu/"+db+"/  /home/ubuntu/p_qdag/test_queries/"+query);
	System.out.println("java -jar -Djava.library.path=/home/ubuntu/p_qdag/solibs/ /home/ubuntu/p_qdag/"+
        "control_scripts_local/gquery.jar /home/ubuntu/"+db+"/  /home/ubuntu/p_qdag/test_queries/"+query);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        //BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        // Read the output from the command
        String resultLine = null;
        String finalResult = "";
        while ((resultLine  = stdInput.readLine()) != null)
           finalResult += resultLine  + "\n";
        // Read any errors from the attempted command
        /*while ((resultLine = stdError.readLine()) != null) 
            System.out.println(resultLine);*/
		return finalResult;
	}

    @GetMapping("/get-metadata")
    public String getMetadata() throws IOException{
        File resource = new ClassPathResource("data/data.json").getFile();
        String metadata = new String(Files.readAllBytes(resource.toPath()));
        return metadata;
    }
    @GetMapping("/run-query")
    public Map<String, String> sendQuery(@RequestParam("db") String db,@RequestParam("queryPath") String queryPath,@RequestParam("resultFile") String resultFile) throws IOException,InterruptedException{
        File dbDir = new File("/home/boumi/RDF_QDAG_TEST_DIR/"+ db);
        File queryFile = new File("/home/boumi/RDF_QDAG_TEST_DIR/watdiv/gStore/"+ queryPath);
        if (!dbDir.exists() || !queryFile.exists()) 
           return new HashMap<>();
        System.out.println("9eweeeeeeeed");
        Process proc = Runtime.getRuntime().exec("java -jar /home/boumi/querySender.jar "+db+" "+queryPath+" "+resultFile);
        proc.waitFor();
        Thread.sleep(4000);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(new FileInputStream("/home/boumi/queryResults/"+resultFile)));
        String resultLine = null;
        String finalResult = "";
        String execTime = "";
        while ((resultLine  = stdInput.readLine()) != null){
           finalResult += resultLine  + "\n";
           execTime = resultLine;
        }
        Map<String, String> result = new HashMap<>();
        result.put("finalResult",finalResult);
        result.put("execTime",execTime);
		return result;
    }
}
