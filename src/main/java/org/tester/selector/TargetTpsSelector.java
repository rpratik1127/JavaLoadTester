package org.tester.selector;

import java.util.Scanner;

/** Reads optional global TPS cap from stdin; 0 means unlimited. */
public class TargetTpsSelector {

    public int selectTargetTps() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("Enter target TPS, or 0 for unlimited: ");

            if (!scanner.hasNextInt()) {
                System.out.println("Invalid input. Please enter a number.");
                scanner.next();
                continue;
            }

            int targetTps = scanner.nextInt();

            if (targetTps < 0) {
                System.out.println("Target TPS cannot be negative.");
                continue;
            }

            return targetTps;
        }
    }
}