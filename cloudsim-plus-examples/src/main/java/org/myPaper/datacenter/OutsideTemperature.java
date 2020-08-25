package org.myPaper.datacenter;

import com.opencsv.CSVReader;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.util.ResourceLoader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class OutsideTemperature {
    private final Datacenter DATACENTER;
    private final Map<Integer, Double> outsideTemperatureMap;
    private Calendar csvFileStartTime = null;
    private long maximumAllowedTime;

    public OutsideTemperature(Datacenter datacenter) {
        DATACENTER = datacenter;
        outsideTemperatureMap = new TreeMap<>();
    }

    public void loadOutsideTemperature(String weatherDataset) throws ParseException, FileNotFoundException {
        CSVReader csvFile = new CSVReader(new FileReader(ResourceLoader.getResourcePath(OutsideTemperature.class, weatherDataset)));
        int line = 0;
        long lastTime = 0;
        for (String[] nextLine : csvFile) {
            if (line > 0) {
                if (csvFileStartTime == null) {
                    setCsvFileStartTime(nextLine[0]);
                }
                long seconds = convertLocalTimeToSeconds(nextLine[0]);
                lastTime = seconds;
                outsideTemperatureMap.put((int) seconds, Double.parseDouble(nextLine[1]));
            }
            line++;
        }

        maximumAllowedTime = lastTime;
    }

    private void setCsvFileStartTime(final String firstTimeDate) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        Date date = dateFormat.parse(firstTimeDate);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 0, 0);

        csvFileStartTime = calendar;
    }

    /**
     * Convert data center local time to seconds.
     *
     * @param localTime data center local time
     * @return local time in seconds
     * @throws ParseException
     */
    private long convertLocalTimeToSeconds(String localTime) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        Date date = dateFormat.parse(localTime);

        return (date.getTime() - csvFileStartTime.getTime().getTime()) / 1000L;
    }

    /**
     * Get datacenter outside temperature at the given time (GMT Timezone).
     *
     * @param time the target time
     * @return outside temperature in centigrade
     */
    public double getOutsideTemperature(double time) {
        int simulationTime = (int) getDatacenter().getLocalTime(time);

        if (simulationTime > maximumAllowedTime) {
            return (double) outsideTemperatureMap.values().toArray()[outsideTemperatureMap.size() - 1];
        }

        double outsideTemperature = Double.NaN;
        for (Map.Entry<Integer, Double> entry : outsideTemperatureMap.entrySet()) {
            if (simulationTime <= entry.getKey()) {
                outsideTemperature = entry.getValue();
                break;
            }
        }

        if (Double.isNaN(outsideTemperature)) {
            throw new IllegalStateException("The outside temperature could not be NaN!");
        }

        return outsideTemperature;
    }

    /**
     * Get current datacenter outside temperature (GMT Timezone).
     *
     * @return outside temperature in centigrade
     */
    public double getOutsideTemperature() {
        return getOutsideTemperature((int) getDatacenter().getSimulation().clock());
    }

    public DatacenterPro getDatacenter() {
        return (DatacenterPro) DATACENTER;
    }
}
