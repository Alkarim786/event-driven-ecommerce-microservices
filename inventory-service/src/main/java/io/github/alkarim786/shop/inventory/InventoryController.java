package io.github.alkarim786.shop.inventory;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/inventory")
class InventoryController {

    record ProductView(String sku, String name, int available, int reserved, int sold, long version) {}

    record RestockRequest(@Min(1) @Max(1_000_000) int quantity) {}

    private static final RowMapper<ProductView> MAPPER = (rs, i) -> new ProductView(rs.getString("sku"),
            rs.getString("name"), rs.getInt("available"), rs.getInt("reserved"), rs.getInt("sold"), rs.getLong("version"));

    private final JdbcTemplate jdbc;

    InventoryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping
    List<ProductView> all() {
        return jdbc.query("SELECT * FROM products ORDER BY sku", MAPPER);
    }

    @GetMapping("/{sku}")
    ResponseEntity<ProductView> one(@PathVariable String sku) {
        return ResponseEntity.of(find(sku));
    }

    /** Admin restock. Relative update, so it is safe to run concurrently with reservations. */
    @PostMapping("/{sku}/restock")
    ResponseEntity<ProductView> restock(@PathVariable String sku, @Valid @RequestBody RestockRequest req) {
        int n = jdbc.update("UPDATE products SET available = available + ?, version = version + 1 WHERE sku = ?",
                req.quantity(), sku);
        return n == 0 ? ResponseEntity.notFound().build() : ResponseEntity.of(find(sku));
    }

    private Optional<ProductView> find(String sku) {
        return jdbc.query("SELECT * FROM products WHERE sku = ?", MAPPER, sku).stream().findFirst();
    }
}
