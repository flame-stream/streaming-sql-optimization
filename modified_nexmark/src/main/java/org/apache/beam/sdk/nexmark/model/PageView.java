package org.apache.beam.sdk.nexmark.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.joda.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

/** View of the page of the auction. */
@DefaultSchema(JavaFieldSchema.class)
@SuppressWarnings({ "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class PageView implements KnownSize, Serializable {

	private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
	private static final Coder<Long> LONG_CODER = VarLongCoder.of();
	private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

	public static final Coder<PageView> CODER = new CustomCoder<PageView>() {
		@Override
		public void encode(PageView value, OutputStream outStream) throws CoderException, IOException {
			LONG_CODER.encode(value.id, outStream);
			LONG_CODER.encode(value.viewer, outStream);
			LONG_CODER.encode(value.auction, outStream);
			INSTANT_CODER.encode(value.dateTime, outStream);
			STRING_CODER.encode(value.extra, outStream);
		}

		@Override
		public PageView decode(InputStream inStream) throws CoderException, IOException {
			long id = LONG_CODER.decode(inStream);
			long viewer = LONG_CODER.decode(inStream);
			long auction = LONG_CODER.decode(inStream);
			Instant dateTime = INSTANT_CODER.decode(inStream);
			String extra = STRING_CODER.decode(inStream);
			return new PageView(id, viewer, auction, dateTime, extra);
		}

		@Override
		public Object structuralValue(PageView v) {
			return v;
		}
	};

	/** Id of pageView. */
	@JsonProperty
	public long id; // primary key

	/** Id of person who viewed the page. */
	@JsonProperty
	public long viewer; // foreign key: Person.id

	/** Id of auction this page is for. */
	@JsonProperty
	public long auction; // foreign key: Auction.id

	/**
	 * Instant at which view was made.
	 */
	@JsonProperty
	public Instant dateTime;

	/** Additional arbitrary payload for performance testing. */
	@JsonProperty
	public String extra;

	// For Avro only.
	@SuppressWarnings("unused")
	public PageView() {

	}

	public PageView(long id, long viewer, long auction, Instant dateTime, String extra) {
		super();
		this.id = id;
		this.viewer = viewer;
		this.auction = auction;
		this.dateTime = dateTime;
		this.extra = extra;
	}

	@Override
	public long sizeInBytes() {
		return 8L + 8L + 8L + 8L + extra.length() + 1L;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id, viewer, auction, dateTime, extra);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageView other = (PageView) obj;
		if (auction != other.auction)
			return false;
		if (dateTime == null) {
			if (other.dateTime != null)
				return false;
		} else if (!dateTime.equals(other.dateTime))
			return false;
		if (extra == null) {
			if (other.extra != null)
				return false;
		} else if (!extra.equals(other.extra))
			return false;
		if (id != other.id)
			return false;
		if (viewer != other.viewer)
			return false;
		return true;
	}

	@Override
	public String toString() {
		try {
			return NexmarkUtils.MAPPER.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Return a copy of person which capture the given annotation. (Used for
	 * debugging).
	 */
	public PageView withAnnotation(String annotation) {
		return new PageView(id, viewer, auction, dateTime, annotation + ": " + extra);
	}

	/** Does person have {@code annotation}? (Used for debugging.) */
	public boolean hasAnnotation(String annotation) {
		return extra.startsWith(annotation + ": ");
	}

	/** Remove {@code annotation} from person. (Used for debugging.) */
	public PageView withoutAnnotation(String annotation) {
		if (hasAnnotation(annotation)) {
			return new PageView(id, viewer, auction, dateTime, extra.substring(annotation.length() + 2));
		}
		return this;
	}
}
