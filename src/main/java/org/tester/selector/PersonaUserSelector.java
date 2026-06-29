package org.tester.selector;

import org.tester.model.Persona;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Interactive CLI for choosing load mode and per-persona values.
 */
public class PersonaUserSelector {

    public PersonaLoadConfig selectLoadConfig(List<Persona> personas) {
        Scanner scanner = new Scanner(System.in);
        LoadInputMode mode = selectInputMode(scanner);

        Map<String, Integer> valuesPerPersona = new LinkedHashMap<>();
        int totalUsers = 0;

        System.out.println();
        System.out.println("Available personas:");
        for (int i = 0; i < personas.size(); i++) {
            System.out.println((i + 1) + ". " + personas.get(i).name);
        }

        System.out.println();

        String valueLabel = mode == LoadInputMode.USERS ? "users" : "total requests";

        for (Persona persona : personas) {
            int value;

            while (true) {
                System.out.print("Enter number of " + valueLabel + " for " + persona.name + ": ");

                if (!scanner.hasNextInt()) {
                    System.out.println("Invalid input. Please enter a number.");
                    scanner.next();
                    continue;
                }

                value = scanner.nextInt();

                if (value < 0) {
                    System.out.println("Value cannot be negative.");
                    continue;
                }

                break;
            }

            if (value > 0) {
                valuesPerPersona.put(persona.name, value);
                if (mode == LoadInputMode.USERS) {
                    totalUsers += value;
                }
            }
        }

        if (mode == LoadInputMode.USERS) {
            PersonaLoadConfig.setTotalUsers(totalUsers);
        }
        return new PersonaLoadConfig(mode, valuesPerPersona);
    }

    private LoadInputMode selectInputMode(Scanner scanner) {
        while (true) {
            System.out.println("Select load input mode:");
            System.out.println("1. Number of virtual users per persona");
            System.out.println("2. Total number of requests per persona");
            System.out.print("Enter choice (1 or 2): ");

            if (!scanner.hasNextInt()) {
                System.out.println("Invalid input. Please enter 1 or 2.");
                scanner.next();
                continue;
            }

            int choice = scanner.nextInt();

            if (choice == 1) {
                return LoadInputMode.USERS;
            }
            if (choice == 2) {
                return LoadInputMode.REQUESTS;
            }

            System.out.println("Invalid choice. Please enter 1 or 2.");
        }
    }
}
