package com.filemigration.reconcile;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the reconciler over HTTP so an operator, or another service, can
 * ask whether the migration is currently complete and correct without
 * reading the databases directly.
 */
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
