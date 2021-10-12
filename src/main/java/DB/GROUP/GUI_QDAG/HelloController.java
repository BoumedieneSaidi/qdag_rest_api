package DB.GROUP.GUI_QDAG;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.io.InputStreamReader;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.core.io.ClassPathResource;
@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class HelloController {
	@GetMapping("/run-query")
	public String runQuery(@RequestParam("query") String query,@RequestParam("db") String db) throws IOException {
        File queryFile = new File("/home/saidib/query.in");
        queryFile.createNewFile();
        FileWriter myWriter = new FileWriter(queryFile);
        myWriter.write(query);
        myWriter.close();
        Process proc = Runtime.getRuntime().exec("java -jar -Djava.library.path=/home/ubuntu/p_qdag/solibs/ /home/ubuntu/p_qdag/"+
        "control_scripts_local/gquery.jar /home/ubuntu/db/  /home/ubuntu/query.in");
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
		return "{\"results\":\""+finalResult+"\"}";
	}

    @GetMapping("/get-metadata")
    public String getMetadata() throws IOException{
        File resource = new ClassPathResource("data/data.json").getFile();
        String metadata = new String(Files.readAllBytes(resource.toPath()));
        return metadata;
    }
}
