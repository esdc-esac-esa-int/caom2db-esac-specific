package esac.esdc.ehst.dl.harvester.log.reader;

public class DefaultAction implements IAction {

    @Override
    public String process(String inputLine) {
        return inputLine;
    }

    @Override
    public boolean criteria(String inputLine) {
        return true;
    }

}
