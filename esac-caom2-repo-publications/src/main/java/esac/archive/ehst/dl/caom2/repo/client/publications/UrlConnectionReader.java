package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

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
        StringBuilder response = new StringBuilder("");
        boolean correct = true;
        URL service = null;
        URLConnection con = null;
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
            if (url.startsWith("https")) {
                con = (HttpsURLConnection) service.openConnection();
            } else {
                con = service.openConnection();
            }
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
                response.append(inputLine);
            }
        } catch (IOException ex) {
            log.error("Error reading output from url '" + url + "': " + ex.getMessage());
            ex.printStackTrace();
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
        return response.toString();
    }

}
