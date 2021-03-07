package org.apache.beam.sdk.nexmark.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

/** Result of Query15. */
@DefaultSchema(JavaFieldSchema.class)
@SuppressWarnings({ "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class RecommendedAuction implements KnownSize, Serializable {

	private static final Coder<Long> LONG_CODER = VarLongCoder.of();
	private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

	public static final Coder<RecommendedAuction> CODER = new CustomCoder<RecommendedAuction>() {
		@Override
		public void encode(RecommendedAuction value, OutputStream outStream) throws CoderException, IOException {
			LONG_CODER.encode(value.id, outStream);
			STRING_CODER.encode(value.emailAddress, outStream);
			LONG_CODER.encode(value.competitor, outStream);
			LONG_CODER.encode(value.auction, outStream);
			LONG_CODER.encode(value.viewedAction, outStream);
		}

		@Override
		public RecommendedAuction decode(InputStream inStream) throws CoderException, IOException {
			long personId = LONG_CODER.decode(inStream);
			String personEmail = STRING_CODER.decode(inStream);
			long competitor = LONG_CODER.decode(inStream);
			long mainAction = LONG_CODER.decode(inStream);
			long viewedAction = LONG_CODER.decode(inStream);
			return new RecommendedAuction(personId, personEmail, competitor, mainAction, viewedAction);
		}

		@Override
		public Object structuralValue(RecommendedAuction v) {
			return v;
		}
	};
	@JsonProperty
	public long id;

	@JsonProperty
	public String emailAddress;

	@JsonProperty
	public long competitor;

	@JsonProperty
	public long auction;

	@JsonProperty
	public long viewedAction;

	public RecommendedAuction(long personId, String personEmail, long competitor, long mainAction, long viewedAction) {
		super();
		this.id = personId;
		this.emailAddress = personEmail;
		this.competitor = competitor;
		this.auction = mainAction;
		this.viewedAction = viewedAction;
	}

	public RecommendedAuction() {
		id = 0;
		emailAddress = null;
		competitor = 0;
		auction = 0;
		viewedAction = 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, emailAddress, competitor, auction, viewedAction);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecommendedAuction other = (RecommendedAuction) obj;
		if (competitor != other.competitor)
			return false;
		if (auction != other.auction)
			return false;
		if (emailAddress == null) {
			if (other.emailAddress != null)
				return false;
		} else if (!emailAddress.equals(other.emailAddress))
			return false;
		if (id != other.id)
			return false;
		if (viewedAction != other.viewedAction)
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

	@Override
	public long sizeInBytes() {
		return 8L + emailAddress.length() + 1L + 8L + 8L + 8L;

	}

}
