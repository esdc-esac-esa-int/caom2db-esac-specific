package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;

/**
 *
 * @author jduran
 *
 */
public class UrlConnectionReader {
    private static final Logger log = Logger.getLogger(UrlConnectionReader.class);

    public static String read(String url) throws MalformedURLException, IOException {
        String response = "";
        boolean correct = true;
        URL service = null;
        HttpsURLConnection con = null;
        BufferedReader in = null;
        InputStream is = null;
        try {
            service = new URL(url);
        } catch (MalformedURLException ex) {
            log.error("Error creating url '" + url + "': " + ex.getMessage());
            correct = false;
        }
        if (!correct) {
            return null;
        }
        try {
            con = (HttpsURLConnection) service.openConnection();
            is = con.getInputStream();
        } catch (IOException ex) {
            log.error("Error opening connection to url '" + url + "': " + ex.getMessage());
            correct = false;
        }
        if (!correct) {
            return null;
        }
        try {
            in = new BufferedReader(new InputStreamReader(is));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response = response.concat(inputLine);
            }
        } catch (IOException ex) {
            log.error("Error reading output from url '" + url + "': " + ex.getMessage());
            correct = false;
        } finally {
            if (in != null) {
                in.close();
            }
            if (is != null) {
                is.close();
            }
        }
        if (!correct) {
            return null;
        }
        return response;
    }

}
