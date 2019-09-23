package io.prestosql.plugin.bigo.udf;

import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.Chars.trimTrailingSpaces;

public class BigoStringFunctions {

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring) {
        if (substring.length() == 0) {
            return 1;
        }

        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return countCodePoints(string, 0, index) + 1;
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.INTEGER) long substring) {
        String str = Long.toString(substring);
        return inStr(string, utf8Slice(str));
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inStr(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.DOUBLE) double substring) {
        String str = Double.toString(substring);
        return inStr(string, utf8Slice(str));
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    @TypeParameter("T")
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring, @SqlType("T") long instance) {
        if (instance <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'instance' must be a positive number.");
        }
        if (substring.length() == 0) {
            return 1;
        }
        int foundInstances = 0;
        int index = -1;
        do {
            index = string.indexOf(substring, index + 1);
            if (index < 0) {
                return 0;
            }
            foundInstances++;
        }
        while (foundInstances < instance);

        return countCodePoints(string, 0, index) + 1;
    }

    // for substr
    public static Slice substring(@SqlType(StandardTypes.VARCHAR) Slice utf8, @SqlType(StandardTypes.BIGINT) long start) {
        if ((start == 0) || utf8.length() == 0) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = utf8.length();

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd = utf8.length();

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

//    @Description("suffix starting at given index")
//    @ScalarFunction("substr")
//    @LiteralParameters("x")
//    @SqlType("char(x)")
//    public static Slice charSubstr(@SqlType("char(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start) {
//        return substr(utf8, start);
//    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrBigint(@SqlType(StandardTypes.BIGINT) long utf8, @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrInt(@SqlType(StandardTypes.INTEGER) long utf8, @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrDouble(@SqlType(StandardTypes.DOUBLE) double utf8, @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Double.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTime(@SqlType(StandardTypes.TIME) long utf8, @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTimeWithZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long utf8,
                                               @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTimestamp(@SqlType(StandardTypes.TIMESTAMP) long utf8,
                                            @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTimeStampWithZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long utf8,
                                                    @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    @Description("suffix starting at given index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrDate(@SqlType(StandardTypes.DATE) long utf8,
                                       @SqlType(StandardTypes.BIGINT) long start) {
        return substring(utf8Slice(Long.toString(utf8)), start);
    }

    public static Slice substring(@SqlType(StandardTypes.VARCHAR) Slice utf8, @SqlType(StandardTypes.BIGINT) long start,
                                  @SqlType(StandardTypes.BIGINT) long length) {
        if (start == 0 || (length <= 0) || (utf8.length() == 0)) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);
        int lengthCodePoints = Ints.saturatedCast(length);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
            if (indexEnd < 0) {
                // after end of string
                indexEnd = utf8.length();
            }

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd;
        if (startCodePoint + lengthCodePoints < codePoints) {
            indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
        } else {
            indexEnd = utf8.length();
        }

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

//    @Description("substring of given length starting at an index")
//    @ScalarFunction("substr")
//    @LiteralParameters("x")
//    @SqlType("char(x)")
//    public static Slice charSubstr(@SqlType("char(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
//        return trimTrailingSpaces(substr(utf8, start, length));
//    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrBigint(@SqlType(StandardTypes.BIGINT) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrInt(@SqlType(StandardTypes.INTEGER) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrDouble(@SqlType(StandardTypes.DOUBLE) double utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Double.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTime(@SqlType(StandardTypes.TIME) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTimeWithZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTimestamp(@SqlType(StandardTypes.TIMESTAMP) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrTimestampWithZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction("substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice charSubstrDate(@SqlType(StandardTypes.DATE) long utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length) {
        return trimTrailingSpaces(substring(utf8Slice(Long.toString(utf8)), start, length));
    }
}
