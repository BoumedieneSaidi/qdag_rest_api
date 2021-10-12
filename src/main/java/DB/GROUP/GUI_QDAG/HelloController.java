package DB.GROUP.GUI_QDAG;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class HelloController {

	@GetMapping("/")
	public String index() throws IOException{
        Process proc = Runtime.getRuntime().exec("java -jar -Djava.library.path=/home/ubuntu/p_qdag/solibs/ /home/ubuntu/p_qdag/control_scripts_local/gquery.jar /home/ubuntu/watdvi100k/  /home/ubuntu/p_qdag/control_scripts_local/queries/watdiv/gStore/C3.in");
        BufferedReader stdInput = new BufferedReader(new 
     InputStreamReader(proc.getInputStream()));
      BufferedReader stdError = new BufferedReader(new 
     InputStreamReader(proc.getErrorStream()));

// Read the output from the command
System.out.println("Here is the standard  of the command:\n");
String s = null;
String res = "";
while ((s = stdInput.readLine()) != null) {
    res += s + "\n";
}
System.out.println(res);
// Read any errors from the attempted command
System.out.println("Here is the standard error of the command (if any):\n");
while ((s = stdError.readLine()) != null) {
    System.out.println(s);
}
		return res;
	}
}
