import java.util.UUID;

public class UndashedUuidCompat {

    public static String toString(UUID uuid) {
        return uuid.toString().replace("-", "");
    }

    public static UUID fromString(String undashed) {
        return UUID.fromString(
                undashed.replaceFirst(
                        "(\\w{8})(\\w{4})(\\w{4})(\\w{4})(\\w{12})",
                        "$1-$2-$3-$4-$5"
                )
        );
    }
}