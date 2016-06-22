package io.datastore.common.execution.stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TextDataStreamTest {

  public static final String PREFIX = "a";
  public static final String DATA = "\nb\nc";
  public static final String UNPROCESSED_DATA = "c";
  public static final String DECODED_VALUE = "a";
  public static final String ENCODED_VALUE = "a\n";

  private TextDataStream textDataStream;

  @Before
  public void setUp() {
    textDataStream = new TextDataStream(DATA, PREFIX);
  }

  @Test
  public void shouldReturnValidUnprocessedData() {
    assertThat(textDataStream.unprocessed()).isEqualTo(UNPROCESSED_DATA);
  }

  @Test
  public void shouldAppendNewLineWhenEncodeResultIsCalled() {
    assertThat(textDataStream.encodeResult(DECODED_VALUE)).isEqualTo(ENCODED_VALUE);
  }

  @Test
  public void shouldRemoveNewLineWhenDecodeResultIsCalled() {
    assertThat(textDataStream.decodeResult(ENCODED_VALUE)).isEqualTo(DECODED_VALUE);
  }

  @Test
  public void shouldAppendEncodedResultWhenAddResultIsCalled() {
    StringBuilder builder = new StringBuilder();
    textDataStream.addResult(builder, DECODED_VALUE);

    assertThat(builder.toString()).isEqualTo(ENCODED_VALUE);
  }

  @Test
  public void shouldReturn2ValuesWhenCalledNextUntilItHasNoElements() {
    Iterator<String> iterator = textDataStream.iterator();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo("a");

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo("b");

    assertThat(iterator.hasNext()).isFalse();
    assertThat(iterator.next()).isNull();
  }
}