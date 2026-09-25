package inventory;

public record ReserveResponse(

        boolean success,

        Integer availableQuantity,

        String message
) {
}
