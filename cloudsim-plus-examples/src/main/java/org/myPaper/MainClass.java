package org.myPaper;

import org.myPaper.programs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

public class MainClass {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class.getSimpleName());
    private enum PROGRAM{BFD, FFD, CRA_DP, OEMACS, UACS, OUR_ACS};
    private static String directory = null;

    public static void main(String[] arg) {
        LOGGER.info("Welcome to CloudSim Plus Simulation Toolkit.");
        LOGGER.info("Please choose one of the following programs for running the simulator: \n{}",
            "1. The BFD (Best Fit Decreasing) algorithm \n" +
                "2. The FFD (First Fit Decreasing) algorithm \n" +
                "3. The CRA-DP Algorithm (Khosravi 2017 paper) \n" +
                "4. The OEMACS Algorithm (Liu 2016 paper) \n" +
                "5. The UACS Algorithm (Liu 2017) \n" +
                "6. Our-ACS Algorithm");

        Scanner scanner = new Scanner(System.in);

        int programId = -1;
        PROGRAM program = null;

        while (program == null) {
            System.out.println("Program Id: " );

            int id = scanner.nextInt();
            scanner.nextLine();

            switch (id) {
                case 1:
                    program = PROGRAM.valueOf("BFD");
                    break;
                case 2:
                    program = PROGRAM.valueOf("FFD");
                    break;
                case 3:
                    program = PROGRAM.valueOf("CRA_DP");
                    break;
                case 4:
                    program = PROGRAM.valueOf("OEMACS");
                    break;
                case 5:
                    program = PROGRAM.valueOf("UACS");
                    break;
                case 6:
                    program = PROGRAM.valueOf("OUR_ACS");
                    break;
                default:
                        LOGGER.warn("The given program Id {} does not exist!", id);
            }
        }

        LOGGER.info("You chose the {} program successfully.", program.toString());

        LOGGER.info("Please insert the directory that you want to save the experimental results.");

        while (directory == null) {
            System.out.println("Directory: ");
            directory = scanner.nextLine();

            if (!Files.exists(Paths.get(directory))) {
                LOGGER.warn("The given directory {} does not exist!", directory);
                directory = null;
            }
        }

        runProgram(program);
    }

    private static void runProgram(final PROGRAM program) {
        if (program == null) {
            throw new IllegalStateException("The program enum could not be null!");
        }

        boolean cloudFederation = askYesNoQuestion("Do you need the cloud federation?");

        switch (program) {
            case BFD:
                new BFDProgram(directory, cloudFederation);
                break;
            case FFD:
                new FFDProgram(directory, cloudFederation);
                break;
            case CRA_DP:
                new CraDpProgram(directory, cloudFederation);
                break;
            case OEMACS:
                new Liu2016Program(directory, cloudFederation);
                break;
            case UACS:
                boolean vmMigration = askYesNoQuestion("Do you need live Vm migration (Vm consolidation)?");
                new Liu2017Program(directory, cloudFederation, vmMigration);
                break;
            case OUR_ACS:
                vmMigration = askYesNoQuestion("Do you need live Vm migration (Vm consolidation)?");
                new OurAcsProgram(directory, cloudFederation, vmMigration);
                break;
            default:
                throw new IllegalStateException("The requested program was not found!");
        }
    }

    private static boolean askYesNoQuestion(final String question) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            LOGGER.info(question + " [y|n]");

            String answer = scanner.nextLine();

            if (answer.equals("y")) {
                return true;
            } else if (answer.equals("n")) {
                return false;
            }else {
                LOGGER.warn("Could not recognise the given answer!");
            }
        }
    }
}
