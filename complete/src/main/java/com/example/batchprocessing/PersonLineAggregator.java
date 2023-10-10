package com.example.batchprocessing;

import org.springframework.batch.item.file.transform.LineAggregator;

public class PersonLineAggregator implements LineAggregator<Person> {
    @Override
    public String aggregate(Person person) {
        return person.getFirstName() + "\n" + person.getLastName();
    }
}

