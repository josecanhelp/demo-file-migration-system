package com.filemigration.reconcile;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the reconciler over HTTP so an operator, or another service, can
 * ask whether the migration is currently complete and correct without
 * reading the databases directly. Scoped to the worker profile, the same
 * process that actually runs the migration: the coordinator process
 * shares this jar and would otherwise register this endpoint too, on a
 * completely separate application context hitting the same databases,
 * which is not a second, independent reconciler worth having, only a
 * confusing duplicate of the one on the worker.
 */
@Profile("worker")
@RestController
public class ReconcileController {

    private final ReconcileService reconcileService;

    public ReconcileController(ReconcileService reconcileService) {
        this.reconcileService = reconcileService;
    }

    @PostMapping("/internal/reconcile")
    public ReconcileResult reconcile() {
        return reconcileService.reconcile();
    }
}
