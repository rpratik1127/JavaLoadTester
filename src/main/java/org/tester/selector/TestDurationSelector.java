package org.tester.selector;

import java.util.Scanner;

/** Reads test duration from stdin. */
public class TestDurationSelector {

    public int selectDurationInSeconds() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("Enter test duration in seconds: ");

            if (!scanner.hasNextInt()) {
                System.out.println("Invalid input. Please enter a number.");
                scanner.next();
                continue;
            }

            int duration = scanner.nextInt();

            if (duration <= 0) {
                System.out.println("Duration must be greater than 0.");
                continue;
            }

            return duration;
        }
    }
}