package org.tester.selector;

import java.util.Scanner;

/** Reads ramp-up duration from stdin (must not exceed test duration). */
public class RampUpSelector {

    public int selectRampUpSeconds(int durationSeconds) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("Enter ramp-up time in seconds: ");

            if (!scanner.hasNextInt()) {
                System.out.println("Invalid input. Please enter a number.");
                scanner.next();
                continue;
            }

            int rampUpSeconds = scanner.nextInt();

            if (rampUpSeconds < 0) {
                System.out.println("Ramp-up time cannot be negative.");
                continue;
            }

            if (rampUpSeconds > durationSeconds) {
                System.out.println("Ramp-up time cannot be greater than test duration.");
                System.out.println("Test duration is: " + durationSeconds + " seconds");
                continue;
            }

            return rampUpSeconds;
        }
    }
}