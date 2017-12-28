package esac.esdc.ehst.dl.harvester.log.reader;

public interface IAction {

    String process(String inputLine);
    boolean criteria(String inputLine);
}
