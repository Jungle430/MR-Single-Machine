package com.Jungle.person.tools;

import java.util.concurrent.CompletableFuture;

/**
 * @author Jungle
 */
public interface Worker {
    CompletableFuture<Boolean> work();
}
