package com.Jungle.person.tools;

import java.util.concurrent.Future;

public interface Worker {
    Future<Boolean> work();
}
